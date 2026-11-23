#!/usr/bin/env python3
"""Copia los archivos que hacen funcionar el DAG y deja el proyecto listo para ejecutarlo.

  dags/bronze_a_silver_clientes.py -> carpeta dags/ del entorno de Cloud Composer (Airflow)
  jobs/bronze_to_silver.py         -> gs://<silver>/codigo/  (de ahí lo lee Dataproc Serverless)

Además: activa las APIs de Dataproc, Secret Manager y BigQuery, crea la tabla de auditoría de
ejecuciones (si no existe) y da a Composer permiso de escritura solo sobre ella, da a Composer y al batch los permisos mínimos
para crearlo, crea la llave de anonimización en Secret Manager (solo si no existe) y activa Private
Google Access en la subred 'default'.

Uso:  python3 despliegues/desplegar.py <REGION> <ENTORNO_COMPOSER>
Ej.:  python3 despliegues/desplegar.py us-east1 st-airflow
"""
import re
import secrets
import subprocess
import sys
import tempfile
from pathlib import Path

PROYECTO = "banco-bogota-515414"
SILVER = "banco-bogota-515414-silver"
SECRETO = "hmac-id-cliente"      # llave de la anonimización; el DAG usa la versión 1
DATASET = "auditoria"
TABLA = "ejecuciones_airflow"
# Columnas de la tabla de auditoría: deben coincidir con las del DAG (hay un test que lo verifica)
COLUMNAS = [
    ("id", "STRING", "NOT NULL"),
    ("nombre_dag", "STRING", "NOT NULL"),
    ("run_id", "STRING", ""),
    ("tipo_ejecucion", "STRING", ""),
    ("fecha_logica", "TIMESTAMP", ""),
    ("fecha_hora_ejecucion", "TIMESTAMP", "NOT NULL"),
    ("fecha_hora_fin", "TIMESTAMP", ""),
    ("duracion_segundos", "INT64", ""),
    ("estado_ejecucion", "STRING", "NOT NULL"),
    ("tarea_fallida", "STRING", ""),
    ("url_log", "STRING", ""),
]
RAIZ = Path(__file__).resolve().parent.parent      # carpeta gcp_simple/
DAG = RAIZ / "dags" / "bronze_a_silver_clientes.py"
JOB = RAIZ / "jobs" / "bronze_to_silver.py"


def _ejecutar(args, capturar, entrada=None, herramienta="gcloud"):
    print("$", herramienta, *args, flush=True)    # la entrada estándar (si hay) nunca se imprime
    try:
        return subprocess.run([herramienta, *args], capture_output=capturar, text=True, input=entrada)
    except FileNotFoundError:
        sys.exit(f"ERROR: no se encontró '{herramienta}'. Ejecute este script desde Cloud Shell.")


def gcloud(*args, tolerar_error=False, entrada=None):
    """Ejecuta un comando; si falla, detiene el despliegue (salvo tolerar_error)."""
    r = _ejecutar(args, capturar=tolerar_error, entrada=entrada)
    if r.returncode != 0 and not tolerar_error:
        sys.exit(f"ERROR: falló el comando (código {r.returncode})")
    return r.returncode


def bq(region, sql):
    """Ejecuta una sentencia SQL en BigQuery con la CLI `bq`; si falla, detiene el despliegue."""
    r = _ejecutar([f"--project_id={PROYECTO}", f"--location={region}", "query", "--use_legacy_sql=false", sql],
                  capturar=False, herramienta="bq")
    if r.returncode != 0:
        sys.exit(f"ERROR: falló el comando bq (código {r.returncode})")


def gcloud_salida(*args):
    """Ejecuta un comando y devuelve lo que imprime."""
    r = _ejecutar(args, capturar=True)
    if r.returncode != 0:
        sys.exit(f"ERROR: falló el comando (código {r.returncode})\n{r.stderr}")
    return r.stdout.strip()


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    region, entorno = sys.argv[1:]
    if not re.fullmatch(r"[a-z]+-[a-z]+[0-9]+", region):
        sys.exit(f"ERROR: región inválida: {region!r} (ej.: us-east1)")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", entorno):
        sys.exit(f"ERROR: nombre de entorno inválido: {entorno!r}")
    for f in (DAG, JOB):
        if not f.exists():
            sys.exit(f"ERROR: falta el archivo {f}")

    # 0. APIs de Dataproc, Secret Manager y BigQuery (si ya están activas no pasa nada).
    gcloud("services", "enable", "dataproc.googleapis.com", "secretmanager.googleapis.com",
           "bigquery.googleapis.com", "--project", PROYECTO)

    # 1. Bucket silver (solo si no existe) y código del job
    if gcloud("storage", "buckets", "describe", f"gs://{SILVER}", "--project", PROYECTO, tolerar_error=True) != 0:
        gcloud("storage", "buckets", "create", f"gs://{SILVER}", "--project", PROYECTO, "--location", region)
    gcloud("storage", "cp", str(JOB), f"gs://{SILVER}/codigo/")

    # 2. DAG a Composer, con la región ya ajustada (se sube una copia; el original no se modifica)
    texto, n = re.subn(r'^REGION = .*$', f'REGION = "{region}"', DAG.read_text(encoding="utf-8"), flags=re.M)
    if n != 1:
        sys.exit("ERROR: no se encontró la línea 'REGION = ...' en el DAG")
    with tempfile.TemporaryDirectory() as tmp:
        copia = Path(tmp) / DAG.name
        copia.write_text(texto, encoding="utf-8")
        gcloud("composer", "environments", "storage", "dags", "import",
               "--environment", entorno, "--location", region, "--project", PROYECTO, "--source", str(copia))

    # 3. Permisos. Sin ellos el DAG falla con "dataproc.batches.create denied".
    #    - Composer (quien lanza el batch): dataproc.editor
    #    - Cuenta del batch (por defecto la de Compute Engine): dataproc.worker
    sa_composer = gcloud_salida("composer", "environments", "describe", entorno, "--location", region,
                                "--project", PROYECTO, "--format", "value(config.nodeConfig.serviceAccount)")
    numero = gcloud_salida("projects", "describe", PROYECTO, "--format", "value(projectNumber)")
    if not re.fullmatch(r"[\w.-]+@[\w.-]+\.gserviceaccount\.com", sa_composer) or not numero.isdigit():
        sys.exit(f"ERROR: no se pudo leer la cuenta de Composer ({sa_composer!r}) o el número del proyecto ({numero!r})")
    sa_batch = f"{numero}-compute@developer.gserviceaccount.com"

    gcloud("projects", "add-iam-policy-binding", PROYECTO, "--member", f"serviceAccount:{sa_composer}",
           "--role", "roles/dataproc.editor", "--condition", "None")
    gcloud("projects", "add-iam-policy-binding", PROYECTO, "--member", f"serviceAccount:{sa_batch}",
           "--role", "roles/dataproc.worker", "--condition", "None")
    if sa_composer != sa_batch:      # si son la misma cuenta no hace falta "actuar como"
        gcloud("iam", "service-accounts", "add-iam-policy-binding", sa_batch, "--project", PROYECTO,
               "--member", f"serviceAccount:{sa_composer}", "--role", "roles/iam.serviceAccountUser")

    # 3b. Llave de la anonimización en Secret Manager. Se crea UNA vez, con un valor aleatorio, y nunca se
    #     sobrescribe: cambiarla cambiaría todos los códigos de cliente. El valor viaja por la entrada
    #     estándar (no por argumentos, que se verían en la lista de procesos) y no se imprime.
    #     Solo la cuenta del batch puede leerla, y solo esta llave (permiso sobre el secreto, no el proyecto).
    if gcloud("secrets", "describe", SECRETO, "--project", PROYECTO, tolerar_error=True) != 0:
        gcloud("secrets", "create", SECRETO, "--project", PROYECTO, "--replication-policy", "automatic")
    if gcloud("secrets", "versions", "describe", "1", "--secret", SECRETO, "--project", PROYECTO,
              tolerar_error=True) != 0:
        gcloud("secrets", "versions", "add", SECRETO, "--project", PROYECTO, "--data-file=-",
               entrada=secrets.token_hex(32))
    gcloud("secrets", "add-iam-policy-binding", SECRETO, "--project", PROYECTO,
           "--member", f"serviceAccount:{sa_batch}", "--role", "roles/secretmanager.secretAccessor")

    # 3c. Tabla de auditoría de ejecuciones del DAG. IF NOT EXISTS: si ya existe no se toca (nunca se
    #     borra ni reemplaza el historial). Composer solo puede escribir en ESA tabla (no en todo el
    #     proyecto) y necesita jobUser para poder ejecutar el INSERT.
    tabla = f"{PROYECTO}.{DATASET}.{TABLA}"
    bq(region, f"CREATE SCHEMA IF NOT EXISTS `{PROYECTO}.{DATASET}` OPTIONS (location = '{region}')")
    columnas = ", ".join(f"{n} {t} {extra}".strip() for n, t, extra in COLUMNAS)
    bq(region, f"CREATE TABLE IF NOT EXISTS `{tabla}` ({columnas})")
    bq(region, f'GRANT `roles/bigquery.dataEditor` ON TABLE `{tabla}` TO "serviceAccount:{sa_composer}"')
    gcloud("projects", "add-iam-policy-binding", PROYECTO, "--member", f"serviceAccount:{sa_composer}",
           "--role", "roles/bigquery.jobUser", "--condition", "None")

    # 4. Dataproc Serverless exige "Private Google Access" en la subred. Es idempotente. Si no hay permiso
    #    sobre la red, o la subred no se llama "default", solo se avisa: lo anterior ya quedó hecho.
    if gcloud("compute", "networks", "subnets", "update", "default", "--region", region, "--project", PROYECTO,
              "--enable-private-ip-google-access", tolerar_error=True) != 0:
        print(f"AVISO: no se pudo activar Private Google Access en la subred 'default' de {region}. Si el batch "
              f"falla por la red, ejecute a mano: gcloud compute networks subnets update <SUBRED> "
              f"--region {region} --enable-private-ip-google-access")

    print("Listo. En unos minutos el DAG 'bronze_a_silver_clientes' aparece en Airflow.")


if __name__ == "__main__":
    main()
