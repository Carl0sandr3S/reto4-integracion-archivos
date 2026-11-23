import hashlib
import hmac
from pathlib import Path

import pytest

import bronze_to_silver as job

DATOS = Path(__file__).parent / "datos"
CLAVE = b"llave-de-prueba-0123456789abcdef"


@pytest.mark.parametrize("original, esperado", [
    ("ANA MARIA TORRES", "A** M**** T*****"),
    ("LAURA GOMEZ", "L**** G****"),
    ("NIÑO PEÑA", "N*** P***"),
    ("  JUAN   CARLOS ", "J*** C*****"),        # espacios de más no filtran nada
    ("X", "X"),
    ("", ""),
    (None, None),
])
def test_enmascarar_nombre(original, esperado):
    assert job.enmascarar_nombre(original) == esperado


def test_pseudonimo_es_hmac_sha256_truncado():
    esperado = hmac.new(CLAVE, b"C0001", hashlib.sha256).hexdigest()[:16]
    assert job.pseudonimizar("C0001", CLAVE) == esperado
    assert len(esperado) == 16 and int(esperado, 16) >= 0


def test_pseudonimo_es_consistente_y_depende_de_la_llave():
    assert job.pseudonimizar("C0001", CLAVE) == job.pseudonimizar("C0001", CLAVE)   # el cruce sigue funcionando
    assert job.pseudonimizar("C0001", CLAVE) != job.pseudonimizar("C0001", b"otra-llave-distinta-123456")
    assert job.pseudonimizar("C0001", CLAVE) != hashlib.sha256(b"C0001").hexdigest()[:16]   # no es un hash simple
    assert job.pseudonimizar(None, CLAVE) is None


def test_con_los_clientes_de_ejemplo_no_hay_colisiones_ni_datos_originales():
    _, clientes = job.leer_dbf((DATOS / "Clientes.dbf").read_bytes())
    ids = [c[0] for c in clientes]
    codigos = [job.pseudonimizar(i, CLAVE) for i in ids]
    assert len(set(codigos)) == len(ids) == 12
    assert not any(i in c for i, c in zip(ids, codigos))
    nombres = [job.enmascarar_nombre(c[1]) for c in clientes]
    assert all("*" in n for n in nombres)
    assert not any(c[1] in n for c, n in zip(clientes, nombres))


def test_los_campos_a_anonimizar_son_los_acordados():
    assert job.CAMPOS_ENMASCARAR == ["NOMBRE"] and job.CAMPOS_PSEUDONIMIZAR == ["ID_CLIENTE"]


@pytest.mark.parametrize("nombre", [
    None, "", "latest", "projects/x/secrets/y/versions/latest",      # versión fija obligatoria
    "projects/x/secrets/y", "http://evil.example/x", "projects/x/secrets/../versions/1",
    "projects/x/secrets/y/versions/1:access?x=1",
])
def test_nombre_de_secreto_invalido_se_rechaza_sin_llamar_a_la_red(nombre):
    with pytest.raises(ValueError):
        job.leer_secreto(nombre)
