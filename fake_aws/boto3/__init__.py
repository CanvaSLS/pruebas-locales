"""boto3 falso para pruebas locales: DynamoDB / SNS / S3 en memoria con las reglas estrictas de AWS."""
import copy
import decimal
import re
import time

from botocore.exceptions import ClientError

from .dynamodb.conditions import _Bool, _Leaf

PAGE_CAP = 1000  # DynamoDB corta cada Query a 1 MB: aquí a 1000 ítems para forzar la paginación interna


def _err(code, msg, op):
    return ClientError({"Error": {"Code": code, "Message": msg}}, op)


def _norm(v, path="item"):
    """Serializa como lo haría DynamoDB: int->Decimal, float PROHIBIDO, set/obj anidados validados."""
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    if isinstance(v, float):
        raise TypeError(f"Float types are not supported. Use Decimal types instead. ({path})")
    if isinstance(v, int):
        return decimal.Decimal(v)
    if isinstance(v, decimal.Decimal):
        return v
    if isinstance(v, (list, tuple)):
        return [_norm(x, f"{path}[]") for x in v]
    if isinstance(v, dict):
        return {k: _norm(x, f"{path}.{k}") for k, x in v.items()}
    raise TypeError(f"Tipo no soportado por DynamoDB: {type(v)} en {path}")


class Tabla:
    def __init__(self, nombre, hash_, rango, tipos, gsis):
        self.nombre, self.hash, self.rango, self.tipos, self.gsis = nombre, hash_, rango, tipos, gsis
        self.items = {}   # (h, r) -> item
        self.ttl = None
        self.llamadas = {"query": 0, "get": 0, "put": 0, "update": 0, "delete": 0}

    # ---- claves
    def _kv(self, d, op):
        if self.rango is None:
            if not isinstance(d.get(self.hash), str) or not d[self.hash]:
                raise _err("ValidationException", f"Key {self.hash} inválida", op)
            return (d[self.hash], "")
        for k in (self.hash, self.rango):
            if k not in d:
                raise _err("ValidationException", f"The provided key element does not match the schema ({k})", op)
            if not isinstance(d[k], str) or d[k] == "":
                raise _err("ValidationException", f"Key {k} debe ser string no vacío", op)
        return (d[self.hash], d[self.rango])

    def _validar_gsi(self, item, op):
        for ix in self.gsis.values():
            for k in (ix["hash"], ix["rango"]):
                if k in item and (not isinstance(item[k], str) or item[k] == ""):
                    raise _err("ValidationException", f"Invalid attribute value type for index key {k}", op)
        for attr, tipo in self.tipos.items():
            if attr in item and tipo == "S" and not isinstance(item[attr], str):
                raise _err("ValidationException", f"Type mismatch for attribute to update/key {attr}", op)

    # ---- API Table
    def get_item(self, Key, **kw):
        self.llamadas["get"] += 1
        it = self.items.get(self._kv(Key, "GetItem"))
        return {"Item": copy.deepcopy(it)} if it else {}

    def _cond(self, expr, existente, op):
        if not expr:
            return
        m = re.fullmatch(r"\s*attribute_(not_)?exists\((\w+)\)\s*", expr)
        if not m:
            raise NotImplementedError(f"ConditionExpression no soportada por el emulador: {expr}")
        ok = (existente is not None and m.group(2) in existente)
        if m.group(1):
            ok = not ok
        if not ok:
            raise _err("ConditionalCheckFailedException", "The conditional request failed", op)

    def put_item(self, Item, ConditionExpression=None, **kw):
        self.llamadas["put"] += 1
        item = _norm(copy.deepcopy(Item))
        k = self._kv(item, "PutItem")
        self._validar_gsi(item, "PutItem")
        self._cond(ConditionExpression, self.items.get(k), "PutItem")
        self.items[k] = item
        return {}

    def delete_item(self, Key, ConditionExpression=None, **kw):
        self.llamadas["delete"] += 1
        k = self._kv(Key, "DeleteItem")
        self._cond(ConditionExpression, self.items.get(k), "DeleteItem")
        self.items.pop(k, None)
        return {}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues=None, ExpressionAttributeNames=None,
                    ConditionExpression=None, ReturnValues="NONE", **kw):
        self.llamadas["update"] += 1
        k = self._kv(Key, "UpdateItem")
        self._cond(ConditionExpression, self.items.get(k), "UpdateItem")
        if not UpdateExpression.startswith("SET "):
            raise NotImplementedError("solo SET")
        nombres, valores = ExpressionAttributeNames or {}, ExpressionAttributeValues or {}
        it = self.items.get(k) or {self.hash: k[0], self.rango: k[1]}
        for asign in UpdateExpression[4:].split(", "):
            izq, der = [x.strip() for x in asign.split("=", 1)]
            if izq in (self.hash, self.rango) or nombres.get(izq) in (self.hash, self.rango):
                raise _err("ValidationException", "Cannot update attribute that is part of the key", "UpdateItem")
            it[nombres.get(izq, izq)] = _norm(copy.deepcopy(valores[der]))
        self._validar_gsi(it, "UpdateItem")
        self.items[k] = it
        return {"Attributes": copy.deepcopy(it)} if ReturnValues == "ALL_NEW" else {}

    def query(self, KeyConditionExpression=None, IndexName=None, FilterExpression=None, Limit=None,
              ExclusiveStartKey=None, ScanIndexForward=True, Select=None, **kw):
        self.llamadas["query"] += 1
        if KeyConditionExpression is None:
            raise _err("ValidationException", "Either the KeyConditions or KeyConditionExpression parameter must be specified", "Query")
        if IndexName:
            if IndexName not in self.gsis:
                raise _err("ValidationException", f"The table does not have the specified index: {IndexName}", "Query")
            hk, rk = self.gsis[IndexName]["hash"], self.gsis[IndexName]["rango"]
        else:
            hk, rk = self.hash, self.rango
        hojas = [l for t, l in KeyConditionExpression.leaves()]
        if any(isinstance(l, (_Bool,)) or not isinstance(l, _Leaf) for l in hojas):
            raise _err("ValidationException", "KeyConditionExpression solo admite AND de condiciones", "Query")
        eq_h = [l for l in hojas if l.attr == hk and l.op == "eq"]
        if len(eq_h) != 1:
            raise _err("ValidationException", f"Query condition missed key schema element: {hk}", "Query")
        otras = [l for l in hojas if l is not eq_h[0]]
        for l in otras:
            if l.attr != rk or l.op not in ("eq", "lt", "lte", "gt", "gte", "between", "begins_with"):
                raise _err("ValidationException", f"Condición de clave inválida sobre {l.attr}", "Query")
        if len(otras) > 1:
            raise _err("ValidationException", "Solo una condición sobre la clave de ordenación", "Query")
        h_val = eq_h[0].vals[0]
        cand = [it for it in self.items.values() if it.get(hk) == h_val and (not IndexName or rk in it)]
        cand = [it for it in cand if all(l.ev(it) for l in otras)]
        orden = (lambda it: (it[rk], it[self.hash], it[self.rango])) if IndexName else (lambda it: it[self.rango])
        cand.sort(key=orden, reverse=not ScanIndexForward)
        if ExclusiveStartKey:
            if ExclusiveStartKey.get(hk) != h_val or ExclusiveStartKey.get(self.hash) is None:
                raise _err("ValidationException", "The provided starting key is invalid", "Query")
            pos = (ExclusiveStartKey.get(self.hash), ExclusiveStartKey.get(self.rango))
            idx = next((i for i, it in enumerate(cand) if (it[self.hash], it[self.rango]) == pos), None)
            if idx is None:
                raise _err("ValidationException", "The provided starting key is invalid", "Query")
            cand = cand[idx + 1:]
        tope = min(Limit, PAGE_CAP) if Limit else PAGE_CAP
        pagina, resto = cand[:tope], cand[tope:]
        lek = None
        if pagina and (resto or (Limit and len(pagina) == Limit)):
            ult = pagina[-1]
            lek = {self.hash: ult[self.hash], self.rango: ult[self.rango]}
            if IndexName:
                lek[hk], lek[rk] = ult[hk], ult[rk]
        filtrados = [it for it in pagina if not FilterExpression or FilterExpression.ev(it)]
        r = {"Count": len(filtrados), "ScannedCount": len(pagina)}
        if Select != "COUNT":
            r["Items"] = copy.deepcopy(filtrados)
        if lek:
            r["LastEvaluatedKey"] = lek
        return r

    def batch_writer(self, overwrite_by_pkeys=None):
        return _Batch(self, overwrite_by_pkeys)


class _Batch:
    def __init__(self, t, ow): self.t, self.ow, self.buf = t, ow, []

    def __enter__(self): return self

    def put_item(self, Item):
        self.buf.append(Item)
        if len(self.buf) >= 25:
            self._flush()

    def _flush(self):
        claves = [tuple(i.get(k) for k in (self.t.hash, self.t.rango)) for i in self.buf]
        if len(set(claves)) != len(claves) and not self.ow:
            raise _err("ValidationException", "Provided list of item keys contains duplicates", "BatchWriteItem")
        vistos = {}
        for i in self.buf:
            vistos[tuple(i.get(k) for k in (self.t.hash, self.t.rango))] = i
        for i in vistos.values():
            self.t.put_item(Item=i)
        self.buf = []

    def __exit__(self, *a):
        if a[0] is None:
            self._flush()


class _Recurso:
    def __init__(self, tablas): self.tablas = tablas
    def Table(self, nombre):
        if nombre not in self.tablas:
            raise _err("ResourceNotFoundException", f"Requested resource not found: {nombre}", "DescribeTable")
        return self.tablas[nombre]


class _SNS:
    def __init__(self, bus): self.bus = bus
    def publish(self, TopicArn, Message, Subject=None, MessageAttributes=None, **kw):
        if not TopicArn.startswith("arn:aws:sns:"):
            raise _err("InvalidParameter", "Invalid parameter: TopicArn", "Publish")
        for k, v in (MessageAttributes or {}).items():
            if v.get("DataType") not in ("String", "Number", "String.Array", "Binary") or "StringValue" not in v:
                raise _err("InvalidParameterValue", f"MessageAttribute {k} inválido", "Publish")
        return self.bus.publicar(TopicArn, Subject, Message, MessageAttributes or {})


class _S3:
    def generate_presigned_url(self, ClientMethod, Params, ExpiresIn=3600):
        if ClientMethod not in ("get_object", "put_object") or "Bucket" not in Params or "Key" not in Params:
            raise ValueError("parámetros de presigned inválidos")
        q = "&".join(f"{k}={v}" for k, v in (("X-Amz-Expires", ExpiresIn), ("X-Amz-Algorithm", "AWS4-HMAC-SHA256")))
        return f"https://{Params['Bucket']}.s3.amazonaws.com/{Params['Key']}?{q}&op={ClientMethod}"


# estado global del "AWS" falso (lo configura emulador.py)
ESTADO = {"tablas": {}, "bus": None}


def resource(nombre, **kw):
    assert nombre == "dynamodb", nombre
    return _Recurso(ESTADO["tablas"])


def client(nombre, **kw):
    if nombre == "sns":
        return _SNS(ESTADO["bus"])
    if nombre == "s3":
        return _S3()
    raise NotImplementedError(nombre)


class session:
    class Session:
        def __init__(self, **kw): pass
        def resource(self, n, **kw): return resource(n)
        def client(self, n, **kw): return client(n)
