import subprocess
from types import SimpleNamespace

import pytest

import desplegar


@pytest.fixture
def gcloud(monkeypatch):
    """Reemplaza subprocess.run: registra los comandos y simula si el bucket existe."""
    estado = SimpleNamespace(llamadas=[], entradas=[], bucket_existe=False, dag_subido=None,
                             secreto_existe=False, version_existe=False,
                             sa_composer="svc-composer@banco-bogota-515414.iam.gserviceaccount.com")

    def falso(cmd, **kw):
        estado.llamadas.append(cmd)
        if kw.get("input") is not None:
            estado.entradas.append((cmd, kw["input"]))
        if cmd[1:3] == ["secrets", "describe"]:
            return SimpleNamespace(returncode=0 if estado.secreto_existe else 1, stdout="", stderr="")
        if cmd[1:4] == ["secrets", "versions", "describe"]:
            return SimpleNamespace(returncode=0 if estado.version_existe else 1, stdout="", stderr="")
        if cmd[1:4] == ["storage", "buckets", "describe"]:
            return SimpleNamespace(returncode=0 if estado.bucket_existe else 1)
        if cmd[1:6] == ["composer", "environments", "storage", "dags", "import"]:
            # el archivo temporal se borra al terminar: se lee aquí, en el momento de subirlo
            estado.dag_subido = open(cmd[cmd.index("--source") + 1], encoding="utf-8").read()
        if cmd[1:3] == ["composer", "environments"] and cmd[3] == "describe":
            return SimpleNamespace(returncode=0, stdout=estado.sa_composer + "\n", stderr="")
        if cmd[1:3] == ["projects", "describe"]:
            return SimpleNamespace(returncode=0, stdout="795386418413\n", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(desplegar.subprocess, "run", falso)
    return estado


def ejecutar(monkeypatch, *args):
    monkeypatch.setattr("sys.argv", ["desplegar.py", *args])
    desplegar.main()


def test_orden_y_comandos(gcloud, monkeypatch):
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    cmds = [" ".join(c) for c in gcloud.llamadas]
    assert cmds[0] == ("gcloud services enable dataproc.googleapis.com secretmanager.googleapis.com "
                       "bigquery.googleapis.com --project banco-bogota-515414")
    assert "storage buckets create gs://banco-bogota-515414-silver" in cmds[2] and "--location us-east1" in cmds[2]
    assert cmds[3].startswith("gcloud storage cp") and cmds[3].endswith("gs://banco-bogota-515414-silver/codigo/")
    assert "bronze_to_silver.py" in cmds[3]
    assert "dags import --environment composer-banco --location us-east1" in cmds[4]


def test_no_crea_el_bucket_si_ya_existe(gcloud, monkeypatch):
    gcloud.bucket_existe = True
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    assert not any(c[1:4] == ["storage", "buckets", "create"] for c in gcloud.llamadas)


def test_el_dag_subido_lleva_la_region_y_el_original_no_cambia(gcloud, monkeypatch):
    original = desplegar.DAG.read_text(encoding="utf-8")
    assert 'REGION = "us-central1"' not in original          # la región nueva debe diferir de la del DAG
    ejecutar(monkeypatch, "us-central1", "composer-banco")
    assert 'REGION = "us-central1"' in gcloud.dag_subido and 'REGION = "us-east1"' not in gcloud.dag_subido
    assert desplegar.DAG.read_text(encoding="utf-8") == original


@pytest.mark.parametrize("args", [[], ["us-east1"], ["x; rm -rf /", "env"], ["mars", "env"], ["us-east1", "Mal Nombre"]])
def test_argumentos_invalidos_no_ejecutan_nada(gcloud, monkeypatch, args):
    with pytest.raises(SystemExit):
        ejecutar(monkeypatch, *args)
    assert gcloud.llamadas == []


def test_gcloud_ausente_da_mensaje_claro(monkeypatch):
    def sin_gcloud(*a, **k):
        raise FileNotFoundError
    monkeypatch.setattr(desplegar.subprocess, "run", sin_gcloud)
    with pytest.raises(SystemExit, match="gcloud"):
        ejecutar(monkeypatch, "us-east1", "composer-banco")


def test_un_comando_fallido_detiene_el_despliegue(monkeypatch):
    llamadas = []

    def falla_el_cp(cmd, **kw):
        llamadas.append(cmd)
        if cmd[1:3] == ["storage", "cp"]:
            return SimpleNamespace(returncode=1)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(desplegar.subprocess, "run", falla_el_cp)
    with pytest.raises(SystemExit, match="falló"):
        ejecutar(monkeypatch, "us-east1", "composer-banco")
    assert not any("composer" in c for c in llamadas)       # no llegó a subir el DAG


def roles(gcloud):
    """(cuenta, rol) de cada add-iam-policy-binding ejecutado."""
    out = []
    for c in gcloud.llamadas:
        if "add-iam-policy-binding" in c:
            out.append((c[c.index("--member") + 1], c[c.index("--role") + 1]))
    return out


def test_permisos_minimos_para_crear_el_batch(gcloud, monkeypatch):
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    composer = "serviceAccount:svc-composer@banco-bogota-515414.iam.gserviceaccount.com"
    batch = "serviceAccount:795386418413-compute@developer.gserviceaccount.com"
    assert roles(gcloud) == [(composer, "roles/dataproc.editor"), (batch, "roles/dataproc.worker"),
                             (composer, "roles/iam.serviceAccountUser"),
                             (batch, "roles/secretmanager.secretAccessor"),
                             (composer, "roles/bigquery.jobUser")]
    # "actuar como" se concede sobre la cuenta del batch, no sobre todo el proyecto
    actas = next(c for c in gcloud.llamadas if c[1:3] == ["iam", "service-accounts"])
    assert actas[4] == "795386418413-compute@developer.gserviceaccount.com"


def test_sin_actuar_como_si_composer_usa_la_cuenta_de_compute(gcloud, monkeypatch):
    gcloud.sa_composer = "795386418413-compute@developer.gserviceaccount.com"
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    assert [r for _, r in roles(gcloud)] == ["roles/dataproc.editor", "roles/dataproc.worker",
                                              "roles/secretmanager.secretAccessor", "roles/bigquery.jobUser"]


def test_nunca_concede_roles_amplios(gcloud, monkeypatch):
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    amplios = {"roles/owner", "roles/editor", "roles/admin", "roles/dataproc.admin", "roles/storage.admin",
               "roles/bigquery.admin", "roles/bigquery.dataEditor"}     # dataEditor solo sobre la tabla, no el proyecto
    assert not amplios & {r for _, r in roles(gcloud)}


def test_cuenta_de_composer_ilegible_detiene_antes_de_conceder_permisos(gcloud, monkeypatch):
    gcloud.sa_composer = ""
    with pytest.raises(SystemExit, match="no se pudo leer"):
        ejecutar(monkeypatch, "us-east1", "composer-banco")
    assert roles(gcloud) == []


def test_activa_private_google_access_en_la_subred(gcloud, monkeypatch):
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    cmds = [" ".join(c) for c in gcloud.llamadas]
    assert any("compute networks subnets update default --region us-east1" in c
               and "--enable-private-ip-google-access" in c for c in cmds)
    assert "subnets update" in cmds[-1]                      # va al final: lo importante ya quedó hecho


def test_si_falla_la_subred_solo_avisa(monkeypatch, capsys):
    def falla_la_subred(cmd, **kw):
        if cmd[1:4] == ["compute", "networks", "subnets"]:
            return SimpleNamespace(returncode=1, stdout="", stderr="sin permiso")
        if cmd[1:3] == ["composer", "environments"] and cmd[3] == "describe":
            return SimpleNamespace(returncode=0, stdout="svc@banco-bogota-515414.iam.gserviceaccount.com", stderr="")
        if cmd[1:3] == ["projects", "describe"]:
            return SimpleNamespace(returncode=0, stdout="795386418413", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(desplegar.subprocess, "run", falla_la_subred)
    ejecutar(monkeypatch, "us-east1", "composer-banco")      # no debe lanzar SystemExit
    assert "AVISO" in capsys.readouterr().out


def test_crea_la_llave_una_vez_y_el_valor_no_aparece_en_los_argumentos(gcloud, monkeypatch):
    monkeypatch.setattr(desplegar.secrets, "token_hex", lambda n: "ab" * n)
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    cmds = [" ".join(c) for c in gcloud.llamadas]
    assert any("secrets create hmac-id-cliente" in c and "--replication-policy automatic" in c for c in cmds)
    (cmd, valor), = gcloud.entradas                       # una sola entrada estándar: la llave
    assert "secrets versions add hmac-id-cliente" in " ".join(cmd) and "--data-file=-" in cmd
    assert valor == "ab" * 32 and len(valor) == 64        # 32 bytes aleatorios en hexadecimal
    assert not any(valor in c for c in cmds)              # nunca viaja por argumentos (se vería en `ps`)


def test_no_sobrescribe_una_llave_existente(gcloud, monkeypatch):
    gcloud.secreto_existe = gcloud.version_existe = True
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    cmds = [" ".join(c) for c in gcloud.llamadas]
    assert not any("secrets create" in c or "secrets versions add" in c for c in cmds)
    assert gcloud.entradas == []                          # cambiarla cambiaría todos los códigos de cliente


def test_el_permiso_sobre_la_llave_es_solo_sobre_ese_secreto(gcloud, monkeypatch):
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    acceso = [c for c in gcloud.llamadas if c[1:3] == ["secrets", "add-iam-policy-binding"]]
    assert len(acceso) == 1 and acceso[0][3] == "hmac-id-cliente"
    assert not any(c[1:3] == ["projects", "add-iam-policy-binding"] and "secretmanager" in " ".join(c)
                   for c in gcloud.llamadas)


def sentencias_bq(gcloud):
    """Los comandos `bq`, con su SQL."""
    return [c for c in gcloud.llamadas if c[0] == "bq"]


def test_crea_dataset_y_tabla_de_auditoria_sin_reemplazar(gcloud, monkeypatch):
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    esquema, tabla, _ = sentencias_bq(gcloud)
    for c in (esquema, tabla):
        assert c[1:3] == ["--project_id=banco-bogota-515414", "--location=us-east1"]
        assert c[3:5] == ["query", "--use_legacy_sql=false"]
    assert esquema[5] == "CREATE SCHEMA IF NOT EXISTS `banco-bogota-515414.auditoria` OPTIONS (location = 'us-east1')"
    sql = tabla[5]
    assert sql.startswith("CREATE TABLE IF NOT EXISTS `banco-bogota-515414.auditoria.ejecuciones_airflow`")
    assert "OR REPLACE" not in sql                      # nunca se borra el historial
    for nombre, tipo, _ in desplegar.COLUMNAS:
        assert f"{nombre} {tipo}" in sql
    for obligatoria in ("id STRING NOT NULL", "nombre_dag STRING NOT NULL", "fecha_hora_ejecucion TIMESTAMP NOT NULL",
                        "estado_ejecucion STRING NOT NULL"):
        assert obligatoria in sql


def test_composer_solo_escribe_en_la_tabla_de_auditoria(gcloud, monkeypatch):
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    sql = sentencias_bq(gcloud)[2][5]
    assert sql == ('GRANT `roles/bigquery.dataEditor` ON TABLE `banco-bogota-515414.auditoria.ejecuciones_airflow` '
                   'TO "serviceAccount:svc-composer@banco-bogota-515414.iam.gserviceaccount.com"')


def test_la_tabla_se_crea_antes_de_la_subred_y_la_subred_sigue_al_final(gcloud, monkeypatch):
    ejecutar(monkeypatch, "us-east1", "composer-banco")
    orden = [c[0] if c[0] == "bq" else " ".join(c[1:4]) for c in gcloud.llamadas]
    assert orden.index("bq") < len(orden) - 1 and "subnets update" in " ".join(gcloud.llamadas[-1])


def test_si_falla_bq_se_detiene_el_despliegue(monkeypatch):
    llamadas = []

    def falla_bq(cmd, **kw):
        llamadas.append(cmd)
        if cmd[0] == "bq":
            return SimpleNamespace(returncode=1, stdout="", stderr="")
        if cmd[1:3] == ["composer", "environments"] and cmd[3] == "describe":
            return SimpleNamespace(returncode=0, stdout="svc@banco-bogota-515414.iam.gserviceaccount.com", stderr="")
        if cmd[1:3] == ["projects", "describe"]:
            return SimpleNamespace(returncode=0, stdout="795386418413", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(desplegar.subprocess, "run", falla_bq)
    with pytest.raises(SystemExit, match="bq"):
        ejecutar(monkeypatch, "us-east1", "composer-banco")
    assert not any("subnets" in " ".join(c) for c in llamadas)      # no siguió adelante
