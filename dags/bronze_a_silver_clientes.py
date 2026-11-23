"""DAG de Airflow (Cloud Composer): espera los DBF en bronze y lanza Dataproc Serverless.
Al final registra el resultado de cada ejecución (exitosa o fallida) en una tabla de BigQuery."""
import logging
import uuid
from datetime import datetime, timezone

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

PROYECTO = "banco-bogota-515414"
REGION = "us-east1"                         # región del Composer st-airflow
BRONZE = "banco-bogota-515414-bronze"
SILVER = "banco-bogota-515414-silver"
CARPETA = "informacion_clientes"
# Llave de la anonimización (versión fija: cambiarla cambia todos los códigos). La crea desplegar.py.
SECRETO = f"projects/{PROYECTO}/secrets/hmac-id-cliente/versions/1"

# Tabla de auditoría (la crea desplegar.py con estas mismas columnas: un test verifica que coincidan)
TABLA = f"{PROYECTO}.auditoria.ejecuciones_airflow"
TIPOS = {                      # columna -> tipo de BigQuery, en el orden de la tabla
    "id": "STRING",                    # identificador único de la fila (uuid)
    "nombre_dag": "STRING",
    "run_id": "STRING",                # identificador de la corrida en Airflow
    "tipo_ejecucion": "STRING",        # scheduled (diaria) / manual
    "fecha_logica": "TIMESTAMP",       # periodo de datos que representa la corrida
    "fecha_hora_ejecucion": "TIMESTAMP",   # cuándo empezó a ejecutarse
    "fecha_hora_fin": "TIMESTAMP",
    "duracion_segundos": "INT64",
    "estado_ejecucion": "STRING",      # EXITOSO / FALLIDO
    "tarea_fallida": "STRING",         # primera tarea que falló (vacío si fue exitosa)
    "url_log": "STRING",               # enlace al log de esa tarea en Airflow
}
TAREA_REGISTRO = "registrar_ejecucion"


def _texto(valor):
    """Los estados de Airflow son enums: devuelve su texto ('failed', 'scheduled', ...)."""
    return getattr(valor, "value", valor)


def construir_fila(dag_run, tareas, ahora, id_fila):
    """Arma la fila de auditoría de una corrida. `tareas`: las tareas de la corrida y su estado.
    Es FALLIDO si alguna tarea falló o no pudo correr por una falla anterior."""
    otras = [t for t in tareas if t.task_id != TAREA_REGISTRO]      # esta tarea no se juzga a sí misma
    falladas = [t for t in otras if _texto(t.state) == "failed"]
    fallido = bool(falladas) or any(_texto(t.state) == "upstream_failed" for t in otras)
    inicio = dag_run.start_date or ahora
    return {
        "id": id_fila,
        "nombre_dag": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "tipo_ejecucion": _texto(dag_run.run_type),
        "fecha_logica": getattr(dag_run, "logical_date", None),
        "fecha_hora_ejecucion": inicio,
        "fecha_hora_fin": ahora,
        "duracion_segundos": int((ahora - inicio).total_seconds()),
        "estado_ejecucion": "FALLIDO" if fallido else "EXITOSO",
        "tarea_fallida": falladas[0].task_id if falladas else None,
        "url_log": getattr(falladas[0], "log_url", None) if falladas else None,
    }


def insertar_fila(cliente, fila):
    """INSERT con parámetros: los valores nunca se pegan en el texto SQL."""
    from google.cloud import bigquery

    columnas = list(TIPOS)
    sql = (f"INSERT INTO `{TABLA}` ({', '.join(columnas)}) "
           f"VALUES ({', '.join('@' + c for c in columnas)})")
    parametros = [bigquery.ScalarQueryParameter(c, TIPOS[c], fila[c]) for c in columnas]
    cliente.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=parametros)).result()


def registrar_ejecucion(**contexto):
    """Guarda el resultado de la corrida en BigQuery. Corre siempre (all_done), también si algo falló.
    Si el pipeline falló, la tarea falla después de registrar: así Airflow no marca la corrida como
    exitosa solo porque esta última tarea terminó bien."""
    from google.cloud import bigquery

    dag_run = contexto["dag_run"]
    fila = construir_fila(dag_run, dag_run.get_task_instances(), datetime.now(timezone.utc), str(uuid.uuid4()))
    insertar_fila(bigquery.Client(project=PROYECTO, location=REGION), fila)   # si falla, la tarea falla y se ve
    logging.info("Ejecución registrada en %s: id=%s estado=%s", TABLA, fila["id"], fila["estado_ejecucion"])
    if fila["estado_ejecucion"] == "FALLIDO":
        raise AirflowException(f"El pipeline falló en la tarea '{fila['tarea_fallida']}'. "
                               f"Quedó registrado en {TABLA} (id {fila['id']}).")


with DAG(
    dag_id="bronze_a_silver_clientes",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Bogota"),
    schedule="@daily",
    catchup=False,
) as dag:

    hay_archivos = GCSObjectsWithPrefixExistenceSensor(
        task_id="hay_archivos_en_bronze",
        bucket=BRONZE,
        prefix=f"{CARPETA}/",
        mode="reschedule",
        poke_interval=300,
    )

    a_silver = DataprocCreateBatchOperator(
        task_id="bronze_a_silver",
        project_id=PROYECTO,
        region=REGION,
        # El id debe ser único: lleva el nº de intento para que "Clear" no reutilice un batch fallido
        batch_id="bronze-silver-{{ ts_nodash | lower }}-{{ ti.try_number }}",
        batch={"pyspark_batch": {
            "main_python_file_uri": f"gs://{SILVER}/codigo/bronze_to_silver.py",
            "args": ["--bronze", f"gs://{BRONZE}/{CARPETA}", "--silver", f"gs://{SILVER}/{CARPETA}",
                     "--secret", SECRETO],
        }},
    )

    registrar = PythonOperator(
        task_id=TAREA_REGISTRO,
        python_callable=registrar_ejecucion,
        trigger_rule=TriggerRule.ALL_DONE,      # corre aunque las anteriores hayan fallado
        retries=0,                              # un reintento duplicaría la fila
    )

    hay_archivos >> a_silver >> registrar
