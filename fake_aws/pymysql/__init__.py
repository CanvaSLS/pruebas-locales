"""pymysql FALSO para pruebas locales: traduce el SQL de MySQL a SQLite en memoria.

Lo suficiente para ejercitar los handlers reales (esquema por tenant = base ATTACH-eada, claves
primarias/foráneas, índices, CASCADE, claves duplicadas -> IntegrityError 1062).
NO valida: sintaxis exacta de MySQL, collations, tipos estrictos, ni permisos de RDS.
"""
import decimal
import re
import sqlite3

from . import cursors, err
from .cursors import DictCursor

ESTADO = {"adjuntas": set(), "consultas": []}   # consultas: SQL ejecutado (para auditar en las pruebas)
_db = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
_db.execute("PRAGMA foreign_keys=ON")
sqlite3.register_adapter(decimal.Decimal, float)
_DECIMALES = {"nota", "puntaje_max"}            # DECIMAL en MySQL -> Decimal en Python


def _traducir(sql):
    """Devuelve la lista de sentencias SQLite equivalentes a UNA sentencia MySQL."""
    s = sql.strip()
    m = re.match(r"CREATE DATABASE IF NOT EXISTS (\w+)", s, re.I)
    if m:
        return [("ATTACH", m.group(1))]
    if re.match(r"CREATE TABLE", s, re.I):
        nombre = re.match(r"CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)", s, re.I)
        esq, tabla = nombre.groups()
        claves = re.findall(r",\s*KEY (\w+) \(([^)]*)\)", s)
        s = re.sub(r",\s*KEY \w+ \([^)]*\)", "", s)
        s = re.sub(r"\)\s*ENGINE=.*$", ")", s, flags=re.S)
        s = re.sub(r"REFERENCES \w+\.", "REFERENCES ", s)      # SQLite: el padre vive en la misma base
        out = [s]
        for idx, cols in claves:
            out.append(f"CREATE INDEX IF NOT EXISTS {esq}.{idx}_{tabla} ON {tabla} ({cols})")
        return out
    s = re.sub(r"^INSERT IGNORE", "INSERT OR IGNORE", s, flags=re.I)
    return [s.replace("%s", "?")]


def _adjuntar(nombre):
    if nombre not in ESTADO["adjuntas"]:
        _db.execute(f"ATTACH DATABASE ':memory:' AS {nombre}")
        ESTADO["adjuntas"].add(nombre)


def _error(e):
    msg = str(e)
    if "UNIQUE" in msg or "PRIMARY" in msg:
        return err.IntegrityError(1062, f"Duplicate entry ({msg})")
    if "FOREIGN" in msg:
        return err.IntegrityError(1452, f"Cannot add or update a child row ({msg})")
    return err.OperationalError(2000, msg)


class Cursor:
    def __init__(self, con, como_dict):
        self.con, self.como_dict, self._filas, self.rowcount, self._cur = con, como_dict, [], -1, _db.cursor()

    def _ejecutar(self, sql, params, muchos):
        ESTADO["consultas"].append(sql)
        try:
            for st in _traducir(sql):
                if isinstance(st, tuple):
                    _adjuntar(st[1])
                    continue
                if muchos:
                    self._cur.executemany(st, params)
                else:
                    self._cur.execute(st, tuple(params or ()))
        except sqlite3.IntegrityError as e:
            raise _error(e) from None
        except sqlite3.OperationalError as e:
            raise err.OperationalError(1064, f"{e} :: {sql[:120]}") from None
        self.rowcount = self._cur.rowcount
        cols = [d[0] for d in (self._cur.description or [])]
        filas = self._cur.fetchall() if cols else []
        conv = lambda c, v: decimal.Decimal(str(v)) if c in _DECIMALES and v is not None else v
        self._filas = [{c: conv(c, v) for c, v in zip(cols, f)} if self.como_dict else tuple(f) for f in filas]

    def execute(self, sql, params=None):
        self._ejecutar(sql, params, False)
        return self.rowcount

    def executemany(self, sql, params):
        self._ejecutar(sql, params, True)
        return self.rowcount

    def fetchall(self):
        f, self._filas = self._filas, []
        return f

    def fetchone(self):
        return self._filas.pop(0) if self._filas else None


class Connection:
    def __init__(self, cursorclass=None):
        self.cursorclass = cursorclass

    def cursor(self):
        return Cursor(self, self.cursorclass is DictCursor)

    def ping(self, reconnect=True):
        pass

    def commit(self):
        pass

    def close(self):
        pass


def connect(host=None, user=None, password=None, port=3306, cursorclass=None, **kw):
    if not (host and user and password):
        raise err.OperationalError(2003, "Can't connect to MySQL server: falta host/usuario/contraseña")
    return Connection(cursorclass)
