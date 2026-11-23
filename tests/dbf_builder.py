"""Constructor mínimo de DBF dBase III para fixtures de prueba."""
import struct


def make_dbf(path, fields, rows, encoding="cp1252", deleted=(), lang=0x03, truncate=0):
    """fields: [(nombre, tipo, largo, decimales)]; rows: lista de listas de str ya formateados."""
    rec_len = 1 + sum(f[2] for f in fields)
    hdr_len = 32 + 32 * len(fields) + 1
    head = struct.pack("<BBBBIHH", 3, 126, 11, 22, len(rows), hdr_len, rec_len) + b"\0" * 17
    head += bytes([lang]) + b"\0\0"
    desc = b""
    for name, typ, ln, dec in fields:
        desc += name.encode("ascii").ljust(11, b"\0") + typ.encode() + b"\0" * 4 + bytes([ln, dec]) + b"\0" * 14
    body = b""
    for i, row in enumerate(rows):
        rec = b"*" if i in deleted else b" "
        for (name, typ, ln, dec), v in zip(fields, row):
            txt = str(v)
            rec += (txt.rjust(ln) if typ in "NF" else txt.ljust(ln)).encode(encoding)[:ln]
        body += rec
    data = head + desc + b"\r" + body + b"\x1a"
    if truncate:
        data = data[:-truncate]
    with open(path, "wb") as fh:
        fh.write(data)
