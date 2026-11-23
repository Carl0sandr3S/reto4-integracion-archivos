from pathlib import Path

from bronze_to_silver import leer_dbf
from dbf_builder import make_dbf

DATOS = Path(__file__).parent / "datos"
CAMPOS = [("ID", "C", 6, 0), ("NOMBRE", "C", 20, 0)]


def test_clientes_y_productos_de_ejemplo():
    nc, clientes = leer_dbf((DATOS / "Clientes.dbf").read_bytes())
    npr, productos = leer_dbf((DATOS / "Productos.dbf").read_bytes())
    assert nc == ["ID_CLIENTE", "NOMBRE", "SEGMENTO", "ESTADO"] and len(clientes) == 12
    assert npr == ["ID_PROD", "ID_CLIENTE", "TIPO_PROD", "SALDO", "ESTADO"] and len(productos) == 20
    assert clientes[0] == ("C0001", "ANA MARIA TORRES", "PERSONAS", "A")     # sin espacios de relleno
    assert productos[0] == ("P0001", "C0001", "AHORROS", "125050.00", "A")


def test_cruce_esperado_con_los_datos_de_ejemplo():
    _, clientes = leer_dbf((DATOS / "Clientes.dbf").read_bytes())
    _, productos = leer_dbf((DATOS / "Productos.dbf").read_bytes())
    ids = {c[0] for c in clientes}
    assert sum(p[1] in ids for p in productos) == 18                          # productos con cliente
    assert [p[0] for p in productos if p[1] not in ids] == ["P0019", "P0020"]  # productos sin cliente


def test_omite_registros_borrados(tmp_path):
    f = tmp_path / "t.dbf"
    make_dbf(f, CAMPOS, [["A1", "UNO"], ["A2", "DOS"], ["A3", "TRES"]], deleted={1})
    assert [r[0] for r in leer_dbf(f.read_bytes())[1]] == ["A1", "A3"]


def test_encoding_cp1252_y_vacios_a_none(tmp_path):
    f = tmp_path / "t.dbf"
    make_dbf(f, CAMPOS, [["A1", "NIÑO PEÑA"], ["A2", ""]])
    filas = leer_dbf(f.read_bytes(), "cp1252")[1]
    assert filas[0][1] == "NIÑO PEÑA" and filas[1][1] is None


def test_archivo_sin_registros(tmp_path):
    f = tmp_path / "t.dbf"
    make_dbf(f, CAMPOS, [])
    assert leer_dbf(f.read_bytes()) == (["ID", "NOMBRE"], [])
