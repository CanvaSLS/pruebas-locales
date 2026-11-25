"""emulador.py · "AWS en memoria" que carga los serverless.yml REALES del proyecto.

Qué valida (sin tocar AWS):
  - tablas, claves y GSIs salen del serverless.yml (si el handler consulta un índice que no existe -> falla)
  - API Gateway: rutas/métodos/autorizador del yml, evento proxy, contexto del authorizer, formato de respuesta
  - SNS (filter policies reales) -> SQS (reintentos, maxReceiveCount, DLQ) -> Lambda consumidora (batchItemFailures)
  - DynamoDB estricto: sin floats, sin Scan, claves string, GSIs dispersos, BatchWrite sin duplicados
RDS (cursos y evaluaciones): usa un pymysql FALSO sobre SQLite en memoria (fake_aws/pymysql). Ejercita el SQL
y la lógica de los handlers, pero NO valida MySQL real, red/VPC/Security Group, ni el driver empaquetado.
No valida: IAM/LabRole, cuotas reales, latencias, ni la sintaxis CloudFormation que solo AWS verifica.
"""
import hashlib
import importlib.util
import json
import os
import re
import sys
import uuid
from collections import defaultdict, deque
from pathlib import Path

import yaml

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent / "fake_aws"))
import boto3  # noqa: E402  (el falso)

CUENTA, REGION, STAGE = "123456789012", "us-east-1", "dev"
SERVICIOS = ["ms-auth", "ms-cursos", "ms-evaluaciones", "ms-analitica", "ms-notificaciones", "ms-practica"]


def _clave_y_default(ref):
    """De `opt:stage, 'dev'` saca ("stage", "dev"); sin coma tras la clave, el default es None."""
    if "," not in ref:
        return ref.split(":", 1)[1].strip(), None
    clave, resto = ref.split(",", 1)
    defecto = resto.strip()
    if len(defecto) >= 2 and defecto[0] == defecto[-1] and defecto[0] in "'\"":
        defecto = defecto[1:-1]
    return clave.split(":", 1)[1].strip(), defecto


def _resolver(v, yml):
    if isinstance(v, dict):
        return {k: _resolver(x, yml) for k, x in v.items()}
    if isinstance(v, list):
        return [_resolver(x, yml) for x in v]
    if not isinstance(v, str):
        return v

    def sub(m):
        ref = m.group(1)
        if ref == "aws:region": return REGION
        if ref == "aws:accountId": return CUENTA
        if ref == "sls:stage": return STAGE
        if ref == "cf:sp-infra-dev.DbEndpoint": return "sp-rds-dev.abc123.us-east-1.rds.amazonaws.com"
        if ref.startswith("AWS::"):                       # pseudo-parámetros de CloudFormation (Fn::Sub)
            pseudo = {"AWS::Region": REGION, "AWS::AccountId": CUENTA, "AWS::Partition": "aws",
                      "AWS::URLSuffix": "amazonaws.com"}
            if ref in pseudo: return pseudo[ref]
        if ref == "ApiGatewayRestApi": return "api"       # id lógico de la REST API, resuelto por CloudFormation
        if ref.startswith("opt:"):
            clave, defecto = _clave_y_default(ref)
            if clave == "stage": return STAGE
            if clave == "region": return REGION
            if defecto is not None: return defecto
        if ref.startswith("env:"):
            clave, defecto = _clave_y_default(ref)
            valor = os.environ.get(clave)
            if valor: return valor
            if defecto is not None: return defecto
            return {"DB_PASSWORD": "ClaveDePrueba2026"}.get(clave, "")
        if ref.startswith("self:"):
            cur = yml
            for p in ref[5:].split("."):
                cur = cur[p]
            return str(_resolver(cur, yml))
        raise KeyError(f"variable sin resolver: ${{{ref}}}")
    previo = None
    while previo != v:                      # resuelve primero las variables anidadas (${cf:...-${sls:stage}...})
        previo, v = v, re.sub(r"\$\{([^${}]+)\}", sub, v)
    return v


class Cola:
    def __init__(self, nombre, vis, maxrec, dlq):
        self.nombre, self.vis, self.maxrec, self.dlq = nombre, vis, maxrec, dlq
        self.msgs = deque()
        self.historial = []


class Bus:
    """SNS: tema + suscripciones SQS con FilterPolicy sobre MessageAttributes."""
    def __init__(self):
        self.subs = []       # (cola, politica)
        self.publicados = []

    def publicar(self, topic_arn, subject, message, attrs):
        mid = str(uuid.uuid4())
        self.publicados.append({"id": mid, "subject": subject, "attrs": attrs, "message": json.loads(message)})
        sobre = {"Type": "Notification", "MessageId": mid, "TopicArn": topic_arn, "Subject": subject,
                 "Message": message, "MessageAttributes": attrs}
        entregado = 0
        for cola, politica in self.subs:
            if all(attrs.get(k, {}).get("StringValue") in vals for k, vals in politica.items()):
                cola.msgs.append({"messageId": str(uuid.uuid4()), "body": json.dumps(sobre), "rec": 0})
                entregado += 1
        return {"MessageId": mid, "_entregas": entregado}


class Emulador:
    def __init__(self):
        self.bus = Bus()
        boto3.ESTADO["bus"] = self.bus
        self.ymls, self.envs, self.mods, self.rutas = {}, {}, {}, []
        self.colas, self.consumidores = {}, {}   # id lógico -> Cola ; cola -> (servicio, funcion, batch)
        self.dlq_de = {}
        self.cuentas = {"tabla": defaultdict(int)}
        for s in SERVICIOS:
            self._cargar_servicio(s)
        self._verificar_common()

    # ------------------------------------------------------------------ carga
    def _cargar_servicio(self, s):
        crudo = yaml.safe_load((RAIZ / s / "serverless.yml").read_text(encoding="utf-8"))
        yml = _resolver(crudo, crudo)
        self.ymls[s] = yml
        self.envs[s] = {k: str(v) for k, v in (yml["provider"].get("environment") or {}).items()}
        for lid, r in (yml.get("resources", {}).get("Resources") or {}).items():
            tipo, p = r["Type"], r["Properties"]
            if tipo == "AWS::DynamoDB::Table":
                self._crear_tabla(p)
            elif tipo == "AWS::SQS::Queue":
                red = p.get("RedrivePolicy")
                self.colas[(s, lid)] = Cola(p["QueueName"], int(p.get("VisibilityTimeout", 30)),
                                            int(red["maxReceiveCount"]) if red else None,
                                            red["deadLetterTargetArn"]["Fn::GetAtt"][0] if red else None)
        for lid, r in (yml.get("resources", {}).get("Resources") or {}).items():
            if r["Type"] == "AWS::SNS::Subscription":
                p = r["Properties"]
                assert p["Protocol"] == "sqs" and p.get("FilterPolicyScope") == "MessageAttributes", lid
                cola = self.colas[(s, p["Endpoint"]["Fn::GetAtt"][0])]
                self.bus.subs.append((cola, p["FilterPolicy"]))
        for fn, f in yml["functions"].items():
            for ev in f.get("events", []):
                if "http" in ev:
                    h = ev["http"]
                    self.rutas.append({"servicio": s, "funcion": fn, "handler": f["handler"],
                                       "metodo": h["method"].upper(), "path": h["path"],
                                       "auth": bool(h.get("authorizer")), "cors": h.get("cors") is True})
                if "sqs" in ev:
                    cola = self.colas[(s, ev["sqs"]["arn"]["Fn::GetAtt"][0])]
                    self.consumidores[id(cola)] = (s, f["handler"], int(ev["sqs"].get("batchSize", 10)),
                                                   ev["sqs"].get("functionResponseType") == "ReportBatchItemFailures", cola)

    def _crear_tabla(self, p):
        tipos = {a["AttributeName"]: a["AttributeType"] for a in p["AttributeDefinitions"]}
        ks = {k["KeyType"]: k["AttributeName"] for k in p["KeySchema"]}
        gsis = {}
        for g in p.get("GlobalSecondaryIndexes", []):
            gk = {k["KeyType"]: k["AttributeName"] for k in g["KeySchema"]}
            assert gk["HASH"] in tipos and gk["RANGE"] in tipos, f"GSI {g['IndexName']} usa atributo sin AttributeDefinitions"
            gsis[g["IndexName"]] = {"hash": gk["HASH"], "rango": gk["RANGE"]}
        assert set(tipos) == {ks["HASH"], *([ks["RANGE"]] if "RANGE" in ks else []), *[v for g in gsis.values() for v in g.values()]}, \
            f"{p['TableName']}: AttributeDefinitions sobrantes/faltantes (CloudFormation las rechaza)"
        assert p["BillingMode"] == "PAY_PER_REQUEST"
        t = boto3.Tabla(p["TableName"], ks["HASH"], ks.get("RANGE"), tipos, gsis)  # sp_tokens_acceso: solo HASH (token)
        t.ttl = (p.get("TimeToLiveSpecification") or {}).get("AttributeName")
        boto3.ESTADO["tablas"][p["TableName"]] = t

    def _verificar_common(self):
        """Los servicios de negocio comparten common.py; ms-auth usa una versión propia.

        Se compara el AST sin el docstring del módulo, que es la única diferencia
        entre ms-cursos/ms-notificaciones y el resto.
        """
        import ast

        def huella(ruta):
            arbol = ast.parse(ruta.read_text(encoding="utf-8"))
            cuerpo = arbol.body
            primero = cuerpo[0] if cuerpo else None
            if (isinstance(primero, ast.Expr) and isinstance(getattr(primero, "value", None), ast.Constant)
                    and isinstance(primero.value.value, str)):
                cuerpo = cuerpo[1:]
            return hashlib.sha256(ast.dump(ast.Module(body=cuerpo, type_ignores=[])).encode()).hexdigest()

        hashes = {s: huella(RAIZ / s / "common.py") for s in SERVICIOS if s != "ms-auth"}
        assert len(set(hashes.values())) == 1, f"common.py difiere entre servicios: {hashes}"

    # ----------------------------------------------------------------- handlers
    def _modulo(self, servicio):
        if servicio not in self.mods:
            self._aplicar_env(servicio)
            d = str(RAIZ / servicio)
            sys.path.insert(0, d)
            try:
                spec = importlib.util.spec_from_file_location(f"h_{servicio.replace('-', '_')}", d + "/handler.py")
                m = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(m)
            finally:
                sys.path.remove(d)
            self.mods[servicio] = m
        return self.mods[servicio]

    def _aplicar_env(self, servicio):
        os.environ.update(self.envs[servicio])
        os.environ["AWS_REGION"] = REGION

    def invocar(self, servicio, handler, evento):
        m = self._modulo(servicio)
        self._aplicar_env(servicio)
        return getattr(m, handler.split(".")[1])(evento, type("Ctx", (), {"aws_request_id": "x"})())

    # ------------------------------------------------------------ API Gateway
    def _buscar_ruta(self, metodo, path, servicio=None):
        for r in self.rutas:
            if r["metodo"] != metodo:
                continue
            patron = "^" + re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", r["path"]) + "$"
            m = re.match(patron, path.strip("/"))
            if m:
                return r, m.groupdict()
        return None, None

    def api(self, metodo, path, token=None, json_body=None, query=None, headers=None, body_raw=None):
        """Simula REST API + Lambda proxy. Devuelve (status, headers, body_json)."""
        ruta, params = self._buscar_ruta(metodo.upper(), path)
        if not ruta:
            return 403, {}, {"message": "Missing Authentication Token"}
        evento = {"resource": "/" + ruta["path"], "path": "/" + path.strip("/"), "httpMethod": metodo.upper(),
                  "headers": dict(headers or {}), "multiValueHeaders": None,
                  "queryStringParameters": query or None, "pathParameters": params or None,
                  "stageVariables": None, "isBase64Encoded": False,
                  "body": body_raw if body_raw is not None else (json.dumps(json_body) if json_body is not None else None),
                  "requestContext": {"stage": STAGE, "httpMethod": metodo.upper()}}
        if token is not None:
            evento["headers"]["Authorization"] = token
        if ruta["auth"]:
            auth_hdr = next((v for k, v in evento["headers"].items() if k.lower() == "authorization"), None)
            if not auth_hdr:
                return 401, {}, {"message": "Unauthorized"}          # TOKEN authorizer sin cabecera
            try:
                pol = self.invocar("ms-auth", "handler.autorizar", {
                    "type": "TOKEN", "authorizationToken": auth_hdr,
                    "methodArn": f"arn:aws:execute-api:{REGION}:{CUENTA}:abc123/{STAGE}/{metodo.upper()}/{path.strip('/')}"})
            except Exception as e:  # noqa: BLE001
                if str(e) == "Unauthorized":
                    return 401, {}, {"message": "Unauthorized"}
                return 500, {}, {"message": "Internal server error"}
            ctxa = pol["context"]
            assert all(isinstance(v, (str, int, float, bool)) for v in ctxa.values()), "context del authorizer solo admite escalares"
            assert pol["policyDocument"]["Statement"][0]["Effect"] == "Allow"
            evento["requestContext"]["authorizer"] = {**ctxa, "principalId": pol["principalId"]}
        try:
            r = self.invocar(ruta["servicio"], ruta["handler"], evento)
        except Exception as e:  # noqa: BLE001
            return 502, {}, {"message": "Internal server error", "_excepcion": repr(e)}
        assert isinstance(r, dict) and isinstance(r.get("statusCode"), int) and isinstance(r.get("body"), str), \
            f"respuesta proxy mal formada (API GW daría 502): {r}"
        return r["statusCode"], r.get("headers", {}), (json.loads(r["body"]) if r["body"] else None)

    # --------------------------------------------------------------- SQS pump
    def bombear(self, maximo=200):
        """Entrega mensajes a las Lambdas consumidoras hasta vaciar colas (con reintentos y DLQ)."""
        pasos = 0
        while pasos < maximo:
            hubo = False
            for cid, (serv, handler, batch, parcial, cola) in self.consumidores.items():
                if not cola.msgs:
                    continue
                hubo = True
                pasos += 1
                lote = [cola.msgs.popleft() for _ in range(min(batch, len(cola.msgs)))]
                for m in lote:
                    m["rec"] += 1
                evento = {"Records": [{"messageId": m["messageId"], "body": m["body"],
                                       "attributes": {"ApproximateReceiveCount": str(m["rec"])},
                                       "eventSource": "aws:sqs"} for m in lote]}
                try:
                    r = self.invocar(serv, handler, evento)
                    fallidos = {f["itemIdentifier"] for f in (r or {}).get("batchItemFailures", [])} if parcial else set()
                except Exception:  # noqa: BLE001
                    fallidos = {m["messageId"] for m in lote}   # lote completo falla si la Lambda lanza
                for m in lote:
                    cola.historial.append((m["messageId"], m["rec"], m["messageId"] in fallidos))
                    if m["messageId"] in fallidos:
                        if cola.maxrec and m["rec"] >= cola.maxrec:
                            self.colas[(serv, cola.dlq)].msgs.append(m)
                        else:
                            cola.msgs.append(m)
            if not hubo:
                return
        raise AssertionError("bucle infinito de eventos: las colas no se vacían")

    def cola(self, servicio, nombre_logico):
        return self.colas[(servicio, nombre_logico)]

    def tabla(self, nombre):
        return boto3.ESTADO["tablas"][nombre]
