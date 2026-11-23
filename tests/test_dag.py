"""Revisión básica del DAG sin Airflow instalado (se simulan sus clases). No reemplaza ejecutarlo en Composer."""
import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

DAG = Path(__file__).resolve().parents[1] / "dags" / "bronze_a_silver_clientes.py"


class Tarea:
    todas = []

    def __init__(self, **kw):
        self.kw, self.siguiente = kw, []
        Tarea.todas.append(self)

    def __rshift__(self, otra):
        self.siguiente.append(otra)
        return otra


class ErrorAirflow(Exception):
    pass


class FakeDAG:
    def __init__(self, **kw):
        self.kw = kw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def modulo(monkeypatch):
    Tarea.todas = []
    for nombre, attrs in {
        "pendulum": {"datetime": lambda *a, **k: None},
        "airflow": {"DAG": FakeDAG},
        "airflow.exceptions": {"AirflowException": ErrorAirflow},
        "airflow.operators": {}, "airflow.operators.python": {"PythonOperator": Tarea},
        "airflow.utils": {}, "airflow.utils.trigger_rule": {"TriggerRule": SimpleNamespace(ALL_DONE="all_done")},
        "airflow.providers": {}, "airflow.providers.google": {}, "airflow.providers.google.cloud": {},
        "airflow.providers.google.cloud.operators": {}, "airflow.providers.google.cloud.sensors": {},
        "airflow.providers.google.cloud.operators.dataproc": {"DataprocCreateBatchOperator": Tarea},
        "airflow.providers.google.cloud.sensors.gcs": {"GCSObjectsWithPrefixExistenceSensor": Tarea},
    }.items():
        m = types.ModuleType(nombre)
        m.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, nombre, m)
    spec = importlib.util.spec_from_file_location("dag_bajo_prueba", DAG)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_dos_tareas_en_orden(modulo):
    sensor, batch, registro = Tarea.todas
    assert sensor.kw["task_id"] == "hay_archivos_en_bronze" and batch.kw["task_id"] == "bronze_a_silver"
    assert sensor.siguiente == [batch] and batch.siguiente == [registro]
    assert sensor.kw["mode"] == "reschedule"            # no ocupa un worker mientras espera


def test_apunta_a_bronze_silver_y_al_job(modulo):
    sensor, batch, _ = Tarea.todas
    assert sensor.kw["bucket"] == "banco-bogota-515414-bronze" and sensor.kw["prefix"] == "informacion_clientes/"
    py = batch.kw["batch"]["pyspark_batch"]
    assert py["main_python_file_uri"] == "gs://banco-bogota-515414-silver/codigo/bronze_to_silver.py"
    assert py["args"] == ["--bronze", "gs://banco-bogota-515414-bronze/informacion_clientes",
                          "--silver", "gs://banco-bogota-515414-silver/informacion_clientes",
                          "--secret", "projects/banco-bogota-515414/secrets/hmac-id-cliente/versions/1"]
    assert batch.kw["project_id"] == "banco-bogota-515414"


def test_batch_id_cambia_con_cada_intento(modulo):
    batch = Tarea.todas[1]
    assert "ti.try_number" in batch.kw["batch_id"]       # evita reutilizar un batch fallido al hacer Clear


# ---------- Registro de ejecuciones en BigQuery ----------
AHORA = datetime(2026, 11, 22, 12, 0, 40, tzinfo=timezone.utc)
INICIO = AHORA - timedelta(seconds=40)


def corrida():
    return SimpleNamespace(dag_id="bronze_a_silver_clientes", run_id="scheduled__2026-11-21", run_type="scheduled",
                           logical_date=INICIO - timedelta(days=1), start_date=INICIO)


def ti(task_id, state, log_url=None):
    return SimpleNamespace(task_id=task_id, state=state, log_url=log_url)


def test_corrida_exitosa(modulo):
    tareas = [ti("hay_archivos_en_bronze", "success"), ti("bronze_a_silver", "success"),
              ti("registrar_ejecucion", "running")]
    f = modulo.construir_fila(corrida(), tareas, AHORA, "id-1")
    assert f["estado_ejecucion"] == "EXITOSO" and f["tarea_fallida"] is None and f["url_log"] is None
    assert f["id"] == "id-1" and f["nombre_dag"] == "bronze_a_silver_clientes"
    assert f["run_id"] == "scheduled__2026-11-21" and f["tipo_ejecucion"] == "scheduled"
    assert f["fecha_hora_ejecucion"] == INICIO and f["fecha_hora_fin"] == AHORA and f["duracion_segundos"] == 40


def test_corrida_fallida_identifica_la_tarea_y_su_log(modulo):
    tareas = [ti("hay_archivos_en_bronze", "success"), ti("bronze_a_silver", "failed", "http://airflow/log?x"),
              ti("registrar_ejecucion", "running")]
    f = modulo.construir_fila(corrida(), tareas, AHORA, "id-2")
    assert f["estado_ejecucion"] == "FALLIDO"
    assert f["tarea_fallida"] == "bronze_a_silver" and f["url_log"] == "http://airflow/log?x"


def test_si_falla_el_sensor_la_causa_es_el_sensor_no_la_tarea_que_no_corrio(modulo):
    tareas = [ti("hay_archivos_en_bronze", "failed"), ti("bronze_a_silver", "upstream_failed"),
              ti("registrar_ejecucion", "running")]
    f = modulo.construir_fila(corrida(), tareas, AHORA, "id-3")
    assert f["estado_ejecucion"] == "FALLIDO" and f["tarea_fallida"] == "hay_archivos_en_bronze"


def test_tarea_que_no_pudo_correr_cuenta_como_fallida_aunque_no_haya_un_failed(modulo):
    tareas = [ti("hay_archivos_en_bronze", "success"), ti("bronze_a_silver", "upstream_failed")]
    f = modulo.construir_fila(corrida(), tareas, AHORA, "id")
    assert f["estado_ejecucion"] == "FALLIDO" and f["tarea_fallida"] is None      # nunca "EXITOSO" por error


def test_la_tarea_de_registro_no_se_juzga_a_si_misma(modulo):
    tareas = [ti("bronze_a_silver", "success"), ti("registrar_ejecucion", "failed")]
    assert modulo.construir_fila(corrida(), tareas, AHORA, "id")["estado_ejecucion"] == "EXITOSO"


def test_estados_como_enum_de_airflow(modulo):
    class Estado:                      # Airflow usa enums cuyo texto está en .value
        def __init__(self, v):
            self.value = v
    tareas = [ti("bronze_a_silver", Estado("failed"))]
    c = corrida()
    c.run_type = Estado("manual")
    f = modulo.construir_fila(c, tareas, AHORA, "id")
    assert f["estado_ejecucion"] == "FALLIDO" and f["tipo_ejecucion"] == "manual"


def test_la_tarea_de_registro_corre_siempre_y_no_se_reintenta(modulo):
    registro = Tarea.todas[2]
    assert registro.kw["task_id"] == "registrar_ejecucion"
    assert registro.kw["python_callable"] is modulo.registrar_ejecucion
    assert registro.kw["trigger_rule"] == "all_done"    # corre aunque las anteriores fallen
    assert registro.kw["retries"] == 0                  # un reintento duplicaría la fila


def test_columnas_del_dag_y_de_la_tabla_coinciden(modulo):
    import desplegar
    assert [(n, t) for n, t, _ in desplegar.COLUMNAS] == list(modulo.TIPOS.items())
    assert modulo.TABLA == f"{desplegar.PROYECTO}.{desplegar.DATASET}.{desplegar.TABLA}"
    assert all(extra in ("", "NOT NULL") for _, _, extra in desplegar.COLUMNAS)


@pytest.fixture
def bigquery_falso(monkeypatch):
    """Simula google.cloud.bigquery y registra lo que se enviaría."""
    reg = SimpleNamespace(clientes=[], consultas=[], falla=False)

    class Trabajo:
        def result(self):
            if reg.falla:
                raise RuntimeError("BigQuery caído")

    class Cliente:
        def __init__(self, **kw):
            reg.clientes.append(kw)

        def query(self, sql, job_config):
            reg.consultas.append((sql, job_config.query_parameters))
            return Trabajo()

    bq = types.ModuleType("google.cloud.bigquery")
    bq.Client = Cliente
    bq.QueryJobConfig = lambda query_parameters: SimpleNamespace(query_parameters=query_parameters)
    bq.ScalarQueryParameter = lambda nombre, tipo, valor: (nombre, tipo, valor)
    for nombre, mod in {"google": types.ModuleType("google"), "google.cloud": types.ModuleType("google.cloud"),
                        "google.cloud.bigquery": bq}.items():
        monkeypatch.setitem(sys.modules, nombre, mod)
    return reg


def contexto(estados):
    c = corrida()
    c.get_task_instances = lambda: [ti(t, e) for t, e in estados.items()]
    return {"dag_run": c}


def test_registra_una_fila_con_parametros_y_no_falla_si_fue_exitosa(modulo, bigquery_falso):
    modulo.registrar_ejecucion(**contexto({"hay_archivos_en_bronze": "success", "bronze_a_silver": "success",
                                          "registrar_ejecucion": "running"}))
    (sql, params), = bigquery_falso.consultas
    assert sql.startswith("INSERT INTO `banco-bogota-515414.auditoria.ejecuciones_airflow`")
    assert [p[0] for p in params] == list(modulo.TIPOS) and [p[1] for p in params] == list(modulo.TIPOS.values())
    assert all("@" + c in sql for c in modulo.TIPOS)
    assert "EXITOSO" not in sql and "bronze_a_silver_clientes" not in sql      # valores van como parámetros
    assert dict((p[0], p[2]) for p in params)["estado_ejecucion"] == "EXITOSO"
    assert bigquery_falso.clientes == [{"project": "banco-bogota-515414", "location": "us-east1"}]


def test_si_el_pipeline_fallo_registra_y_despues_falla_la_tarea(modulo, bigquery_falso):
    with pytest.raises(ErrorAirflow, match="bronze_a_silver"):
        modulo.registrar_ejecucion(**contexto({"hay_archivos_en_bronze": "success", "bronze_a_silver": "failed",
                                              "registrar_ejecucion": "running"}))
    (_, params), = bigquery_falso.consultas             # primero quedó registrado
    assert dict((p[0], p[2]) for p in params)["estado_ejecucion"] == "FALLIDO"


def test_si_no_se_puede_registrar_el_error_no_se_oculta(modulo, bigquery_falso):
    bigquery_falso.falla = True
    with pytest.raises(RuntimeError, match="BigQuery"):
        modulo.registrar_ejecucion(**contexto({"bronze_a_silver": "success"}))
