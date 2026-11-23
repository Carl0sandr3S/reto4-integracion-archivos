"""Job PySpark (Dataproc Serverless): lee los DBF de bronze, relaciona productos con clientes
y escribe el resultado en silver como archivos .txt delimitados (con encabezado).

Silver solo recibe datos ANONIMIZADOS:
  - NOMBRE      -> enmascarado          ANA MARIA TORRES -> A** M**** T*****
  - ID_CLIENTE  -> pseudonimizado       C0001            -> 16 hex (HMAC-SHA256 con llave secreta)
El cruce se hace con los datos originales y se anonimiza justo antes de escribir. La llave se lee
de Secret Manager; si no se indica `--secret` el job falla (nunca escribe datos sin anonimizar).
"""
import argparse
import hashlib
import hmac
import re
import struct

CAMPOS_ENMASCARAR = ["NOMBRE"]
CAMPOS_PSEUDONIMIZAR = ["ID_CLIENTE"]
LARGO_SEUDONIMO = 16          # caracteres hexadecimales (64 bits): sin colisiones prácticas para millones de IDs


def leer_dbf(datos, encoding="cp1252"):
    """Lee un DBF completo (bytes). Devuelve (nombres_de_campo, lista_de_tuplas). Omite borrados."""
    nrec, largo_cab, largo_reg = struct.unpack("<IHH", datos[4:12])
    campos, pos = [], 32
    while datos[pos] != 0x0D:                       # descriptores de campo (32 bytes c/u)
        campos.append((datos[pos:pos + 11].split(b"\0")[0].decode(), datos[pos + 16]))
        pos += 32
    filas = []
    for i in range(nrec):
        reg = datos[largo_cab + i * largo_reg: largo_cab + (i + 1) * largo_reg]
        if reg[:1] == b"*":                         # marcado como borrado
            continue
        p, fila = 1, []
        for _, largo in campos:
            fila.append(reg[p:p + largo].decode(encoding).strip() or None)
            p += largo
        filas.append(tuple(fila))
    return [n for n, _ in campos], filas


def enmascarar_nombre(valor):
    """Deja la primera letra de cada palabra y oculta el resto: 'ANA MARIA' -> 'A** M****'."""
    if valor is None:
        return None
    return " ".join(p[0] + "*" * (len(p) - 1) for p in valor.split())


def pseudonimizar(valor, clave):
    """HMAC-SHA256 con llave secreta, truncado. Mismo valor + misma llave = mismo código, así que los
    cruces y conteos por cliente siguen funcionando. Sin la llave no se puede recalcular ni adivinar
    (un hash simple de 'C0001' se revierte probando todos los IDs posibles)."""
    if valor is None:
        return None
    return hmac.new(clave, valor.encode("utf-8"), hashlib.sha256).hexdigest()[:LARGO_SEUDONIMO]


def leer_secreto(nombre):
    """Lee un secreto de Secret Manager con la cuenta del batch. Solo librería estándar.

    `nombre`: projects/<proyecto>/secrets/<secreto>/versions/<versión>. Se usa una versión fija
    (no 'latest') porque cambiar la llave cambia todos los códigos y rompería la continuidad."""
    import base64
    import json
    import urllib.request

    if not re.fullmatch(r"projects/[A-Za-z0-9_-]+/secrets/[A-Za-z0-9_-]+/versions/[0-9]+", nombre or ""):
        raise ValueError("--secret debe tener la forma projects/<proyecto>/secrets/<nombre>/versions/<número>")
    url_token = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
    peticion = urllib.request.Request(url_token, headers={"Metadata-Flavor": "Google"})
    token = json.load(urllib.request.urlopen(peticion, timeout=10))["access_token"]
    peticion = urllib.request.Request(f"https://secretmanager.googleapis.com/v1/{nombre}:access",
                                      headers={"Authorization": f"Bearer {token}"})
    clave = base64.b64decode(json.load(urllib.request.urlopen(peticion, timeout=20))["payload"]["data"])
    if len(clave) < 16:
        raise ValueError("La llave del secreto es demasiado corta (mínimo 16 bytes)")
    return clave


def anonimizar(df, clave):
    """Aplica la anonimización a las columnas sensibles que existan en `df`."""
    from pyspark.sql import functions as F, types as T

    mascara = F.udf(enmascarar_nombre, T.StringType())
    seudonimo = F.udf(lambda v: pseudonimizar(v, clave), T.StringType())
    for c in CAMPOS_ENMASCARAR:
        if c in df.columns:
            df = df.withColumn(c, mascara(F.col(c)))
    for c in CAMPOS_PSEUDONIMIZAR:
        if c in df.columns:
            df = df.withColumn(c, seudonimo(F.col(c)))
    return df


def escribir_txt(spark, df, destino, sep, ordenar_por="ID_PROD"):
    """Escribe `df` como UN archivo .txt delimitado, con encabezado, en `destino` (gs://.../nombre.txt).

    Spark escribe carpetas con archivos part-*; aquí se escribe a una carpeta temporal y el único
    part-* se renombra al nombre final. Los nulos salen vacíos y, si un valor trae el delimitador,
    se cambia por un espacio para no descuadrar las columnas.
    """
    from pyspark.sql import functions as F

    columnas = df.columns
    limpio = lambda c: F.coalesce(F.regexp_replace(F.col(c).cast("string"), re.escape(sep), " "), F.lit(""))
    datos = df.orderBy(ordenar_por if ordenar_por in columnas else columnas[0]).select(F.concat_ws(sep, *[limpio(c) for c in columnas]).alias("value"))
    encabezado = spark.createDataFrame([(sep.join(columnas),)], ["value"])
    tmp = destino + "_tmp"
    # union conserva el orden de particiones (encabezado primero) y coalesce(1) las junta en ese orden
    encabezado.unionByName(datos).coalesce(1).write.mode("overwrite").text(tmp)

    Path = spark._jvm.org.apache.hadoop.fs.Path
    fs = Path(tmp).getFileSystem(spark._jsc.hadoopConfiguration())
    partes = [e.getPath() for e in fs.listStatus(Path(tmp)) if e.getPath().getName().startswith("part-")]
    if len(partes) != 1:
        raise RuntimeError(f"Se esperaba 1 archivo part-* en {tmp} y hay {len(partes)}")
    fs.delete(Path(destino), False)                     # reemplaza el resultado anterior, si existe
    if not fs.rename(partes[0], Path(destino)):
        raise RuntimeError(f"No se pudo renombrar a {destino}")
    fs.delete(Path(tmp), True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", required=True)   # gs://bucket/informacion_clientes
    ap.add_argument("--silver", required=True)   # gs://bucket/informacion_clientes
    ap.add_argument("--encoding", default="cp1252")
    ap.add_argument("--delimiter", default="|")      # delimitador del .txt de salida
    ap.add_argument("--secret", required=True,       # llave de la anonimización (Secret Manager)
                    help="projects/<proyecto>/secrets/<nombre>/versions/<número>")
    a = ap.parse_args()
    if len(a.delimiter) != 1:
        ap.error("--delimiter debe ser un solo carácter")

    clave = leer_secreto(a.secret)        # falla aquí, antes de leer datos, si no hay llave
    from pyspark.sql import SparkSession, functions as F, types as T
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
    sc = spark.sparkContext

    def cargar(patron):
        archivos = sc.binaryFiles(f"{a.bronze.rstrip('/')}/{patron}")
        nombres = archivos.map(lambda kv: leer_dbf(kv[1], a.encoding)[0]).first()
        filas = archivos.flatMap(lambda kv: leer_dbf(kv[1], a.encoding)[1])
        return spark.createDataFrame(filas, T.StructType([T.StructField(n, T.StringType()) for n in nombres]))

    clientes = cargar("[Cc]lientes*.[Dd][Bb][Ff]").withColumnRenamed("ESTADO", "ESTADO_CLIENTE")
    productos = cargar("[Pp]roductos*.[Dd][Bb][Ff]").withColumnRenamed("ESTADO", "ESTADO_PRODUCTO")
    productos = productos.withColumn("SALDO", F.col("SALDO").cast("decimal(12,2)"))

    cruce = productos.join(clientes, "ID_CLIENTE", "inner")                 # producto + su cliente
    huerfanos = productos.join(clientes, "ID_CLIENTE", "left_anti")         # productos sin cliente

    # Anonimizar justo antes de escribir: lo que llega a silver nunca lleva el dato original
    silver = a.silver.rstrip("/")
    escribir_txt(spark, anonimizar(cruce, clave), f"{silver}/cliente_producto.txt", a.delimiter)
    escribir_txt(spark, anonimizar(huerfanos, clave), f"{silver}/productos_sin_cliente.txt", a.delimiter)
    print(f"clientes={clientes.count()} productos={productos.count()} "
          f"cruzados={cruce.count()} productos_sin_cliente={huerfanos.count()}")


if __name__ == "__main__":
    main()
