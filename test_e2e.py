#!/usr/bin/env python3
"""test_e2e.py · prueba de punta a punta SIN AWS: carga el seed, levanta los 6 serverless.yml reales en el
emulador y recorre aislamiento multi-tenant, CRUD, paginación, EDA (SNS->SQS->Lambda), DLQ e idempotencia.

  python3 pruebas-locales/test_e2e.py            # todo
  python3 pruebas-locales/test_e2e.py -v         # con detalle de cada verificación
"""
import contextlib
import io
import json
import re
import subprocess
import sys
import time
import traceback
from pathlib import Path

AQUI = Path(__file__).resolve().parent
RAIZ = AQUI.parent
sys.path.insert(0, str(AQUI))
sys.path.insert(0, str(RAIZ / "datos-seed"))
sys.path.insert(0, str(RAIZ / "ms-cursos"))   # common.py y sqldb.py (idénticos en todos los servicios)
import emulador  # noqa: E402
import seed  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

boto3 = emulador.boto3
PASS = "Demo2026!"
VERBOSE = "-v" in sys.argv
E = None
TOK = {}
RESULTADOS = []


def prueba(nombre):
    def deco(fn):
        RESULTADOS.append((nombre, fn))
        return fn
    return deco


def ok(cond, msg):
    if not cond:
        raise AssertionError(msg)
    if VERBOSE:
        print(f"      · {msg}")


def login(tenant, email, pwd=PASS):
    s, _, b = E.api("POST", "auth/login", json_body={"tenant_id": tenant, "email": email, "password": pwd})
    return s, b


def tok(tenant, email):
    k = (tenant, email)
    if k not in TOK:
        s, b = login(tenant, email)
        assert s == 200, f"login {email}: {s} {b}"
        TOK[k] = "Bearer " + b["token"]
    return TOK[k]


def api(m, p, who=None, **kw):
    t = tok(*who) if isinstance(who, tuple) else who
    s, h, b = E.api(m, p, token=t, **kw)
    return s, b


def sql(servicio, tenant, consulta, params=()):
    """Consulta directa a la RDS falsa (para verificar lo que los handlers dejaron en la base)."""
    import sqldb
    E._aplicar_env("ms-cursos" if servicio == "cursos" else "ms-evaluaciones")
    cur = sqldb.conexion().cursor()
    cur.execute(consulta.format(s=sqldb.esquema(servicio, tenant)), params)
    return cur.fetchall()


UTEC_DOC = ("UTEC", "profe@utec.edu.pe")
UTEC_ADM = ("UTEC", "admin@utec.edu.pe")
UTEC_ANA = ("UTEC", "ana.torres@utec.edu.pe")
UTEC_VAL = ("UTEC", "valeria.mendoza@utec.edu.pe")
NORTE_DOC = ("NORTE", "profe@inorte.edu.pe")
NORTE_ANA = ("NORTE", "ana.torres@inorte.edu.pe")


# ======================================================================= 0. infraestructura
@prueba("INFRA · rutas, autorizador, CORS y handlers existentes")
def t_infra():
    publicas = sorted((r["metodo"], r["path"]) for r in E.rutas if not r["auth"])
    ok(publicas == [("POST", "auth/login"), ("POST", "auth/registro")], f"solo login/registro son públicas: {publicas}")
    ok(all(r["cors"] for r in E.rutas), "todas las rutas declaran cors: true")
    ok(len(E.rutas) >= 29, f"{len(E.rutas)} endpoints publicados en 6 API Gateways")
    for r in E.rutas:
        ok(hasattr(E._modulo(r["servicio"]), r["handler"].split(".")[1]), f"existe {r['servicio']}:{r['handler']}")
    for s in emulador.SERVICIOS:
        y = E.ymls[s]
        ok(y["provider"]["iam"]["role"].endswith(":role/LabRole"), f"{s} usa LabRole (Learner Lab)")
    ok(not hasattr(boto3.Tabla, "scan"), "el emulador no ofrece Scan: si el código lo usara, fallaría")
    codigo = "".join(p.read_text() for p in RAIZ.glob("ms-*/*.py"))
    ok(".scan(" not in codigo, "ningún microservicio llama a .scan()")
    pol = {c.nombre: p["evento"] for c, p in E.bus.subs}
    ok("AlumnoEnRiesgo" not in pol["sp-analitica-dev"], f"filtro de analítica {pol['sp-analitica-dev']}: no consume su propia salida (sin bucles)")
    ok(set(pol["sp-notificaciones-dev"]) == {"NotaPublicada", "EntregaRegistrada", "ModuloPublicado", "AlumnoEnRiesgo", "QuizCompletado"}, "notificaciones se suscribe a los 5 eventos")
    ok(pol["sp-practica-dev"] == ["ModuloPublicado"], "práctica solo consume ModuloPublicado")
    ok("TOPIC_ARN" in E.envs["ms-cursos"] and "BUCKET" in E.envs["ms-cursos"], "ms-cursos recibe TOPIC_ARN y BUCKET")


@prueba("INFRA · seed: esquema válido, tenants disjuntos y mismos conteos")
def t_seed():
    por = {}
    E._aplicar_env("ms-cursos")
    for t in ("UTEC", "NORTE", "ANDINO"):
        datos, niveles = seed.generar_tenant(t, 300, AHORA)
        seed.cargar_sql(t, datos)                                 # RDS: esquema por tenant (INSERT IGNORE)
        for tabla, items in datos.items():
            if tabla in seed.SQL_TABLAS:
                servicio, sql_tabla, _ = seed.SQL_TABLAS[tabla]
                n = sql(servicio, t, f"SELECT COUNT(*) AS n FROM {{s}}.{sql_tabla}")[0]["n"]
                ok(n == len(items), f"{tabla}: {n} filas en RDS = {len(items)} generadas (ninguna perdida por clave duplicada)")
            else:
                for it in items:
                    boto3.ESTADO["tablas"][tabla].put_item(Item=it)   # valida claves, GSI, floats
            por.setdefault(tabla, {})[t] = len(items)
    seed.cargar_sql("UTEC", seed.generar_tenant("UTEC", 300, AHORA)[0])   # repetir el seed no duplica ni falla
    ok(sql("cursos", "UTEC", "SELECT COUNT(*) AS n FROM {s}.cursos")[0]["n"] == por["sp_cursos"]["UTEC"], "repetir el seed no duplica filas en RDS")
    for tabla, d in por.items():
        ok(len(set(d.values())) == 1, f"{tabla}: mismo nº de filas en los 3 tenants {d}")
    for nombre, t in boto3.ESTADO["tablas"].items():
        if nombre in ("sp_tokens_acceso", "sp_intentos"):
            continue
        ok(all(it["tenant_id"] == k[0] for k, it in t.items.items()), f"{nombre}: PK = tenant_id")


@prueba("INFRA · el semáforo del seed coincide con la regla de ms-analitica")
def t_regla_igual():
    riesgo = emulador.importlib.util.spec_from_file_location("riesgo_real", RAIZ / "ms-analitica" / "riesgo.py")
    m = emulador.importlib.util.module_from_spec(riesgo)
    riesgo.loader.exec_module(m)
    tabla = boto3.ESTADO["tablas"]["sp_indicadores"]
    n = 0
    for it in tabla.items.values():
        res = {"promedio": it["promedio"], "n_tardias": it["n_tardias"], "caida_ultima": it["caida_ultima"]}
        puntaje, nivel, _ = m.calcular_riesgo({k: (None if v is None else float(v)) for k, v in res.items()}) \
            if False else m.calcular_riesgo(res)
        ok(nivel == it["nivel"] and puntaje == it["puntaje"], f"{it['tenant_id']}/{it['alumno_id']} {nivel}={it['nivel']}")
        n += 1
        if n >= 400:
            break


# ======================================================================= 1. autenticación
@prueba("AUTH · login con claves sembradas, errores uniformes y registro")
def t_auth():
    s, b = login("UTEC", "profe@utec.edu.pe")
    ok(s == 200 and b["usuario"]["rol"] == "docente" and b["usuario"]["tenant_id"] == "UTEC", "docente UTEC inicia sesión con Demo2026!")
    s1, b1 = login("UTEC", "profe@utec.edu.pe", "mala-clave")
    s2, b2 = login("UTEC", "noexiste@utec.edu.pe")
    ok(s1 == s2 == 401 and b1["error"] == b2["error"], "clave mala y usuario inexistente dan el mismo 401")
    s3, _ = login("NORTE", "profe@utec.edu.pe")
    ok(s3 == 401, "el email de UTEC no existe en NORTE (las credenciales no cruzan tenants)")
    s4, b4 = login("OTRO", "x@y.com")
    ok(s4 == 400, "tenant desconocido -> 400")
    nuevo = {"tenant_id": "NORTE", "email": "nuevo@inorte.edu.pe", "nombre": "Nuevo Alumno", "password": "ClaveSegura1"}
    s, _, b = E.api("POST", "auth/registro", json_body=nuevo)
    ok(s == 201 and b["user_id"].startswith("A"), "registro de alumno -> 201")
    s, _, b = E.api("POST", "auth/registro", json_body=nuevo)
    ok(s == 409, "mismo email en el mismo tenant -> 409")
    s, _, b = E.api("POST", "auth/registro", json_body={**nuevo, "tenant_id": "ANDINO"})
    ok(s == 201, "el mismo email SÍ puede existir en otro tenant")
    s, _, b = E.api("POST", "auth/registro", json_body={**nuevo, "email": "adm@x.com", "rol": "admin"})
    ok(s == 400 and b["error"] == "ROL_INVALIDO", "no se puede auto-registrar un admin")
    s, _, b = E.api("POST", "auth/registro", json_body={**nuevo, "email": "d@x.com", "password": "123"})
    ok(s == 400, "contraseña débil -> 400")
    s, b = api("GET", "auth/me", UTEC_ANA)
    ok(s == 200 and b["usuario"]["tenant_id"] == "UTEC" and b["usuario"]["rol"] == "alumno", "/auth/me devuelve la identidad del token")
    u = boto3.ESTADO["tablas"]["sp_usuarios"].items[("NORTE", next(i for (t, i) in boto3.ESTADO["tablas"]["sp_usuarios"].items if t == "NORTE")) ]
    ok(u["pwd_hash"] != PASS and len(u["salt"]) >= 16, "la contraseña se guarda como hash con salt")


@prueba("AUTH · authorizer: sin token, token falso, expirado y prefijo Bearer")
def t_authorizer():
    ok(E.api("GET", "cursos")[0] == 401, "sin Authorization -> 401")
    ok(E.api("GET", "cursos", token="Bearer basura")[0] == 401, "token inventado -> 401")
    ok(E.api("GET", "cursos", token="")[0] == 401, "cabecera vacía -> 401")
    s, b = login("UTEC", "ana.torres@utec.edu.pe")
    crudo = b["token"]
    ok(E.api("GET", "cursos", token=crudo)[0] == 200, "token sin 'Bearer' también vale")
    fila = boto3.ESTADO["tablas"]["sp_tokens_acceso"].items[(crudo, "")]
    fila["expira"] = boto3.decimal.Decimal(int(time.time()) - 5)   # DynamoDB TTL puede tardar: el authorizer lo revisa igual
    ok(E.api("GET", "cursos", token="Bearer " + crudo)[0] == 401, "token expirado aunque la fila siga en la tabla -> 401")
    ok(fila["ttl"] is not None and E.tabla("sp_tokens_acceso").ttl == "ttl", "la tabla de tokens declara TTL sobre `ttl`")


# ======================================================================= 2. multi-tenancy
@prueba("TENANT · un tenant no ve, modifica ni borra datos del otro (T-ISO)")
def t_aislamiento():
    s, b = api("GET", "cursos", UTEC_ADM)
    cu = {c["curso_id"] for c in b["cursos"]}
    s, b = api("GET", "cursos", NORTE_DOC)
    ok(s == 200, "NORTE lista cursos")
    s, b = api("GET", "cursos", ("NORTE", "admin@inorte.edu.pe"))
    cn = {c["curso_id"] for c in b["cursos"]}
    ok(all(c["tenant_id"] == "NORTE" for c in b["cursos"]), "NORTE solo recibe filas con tenant_id=NORTE")
    # mismo curso_id (C0001) existe en ambos tenants con datos distintos
    s1, b1 = api("GET", "cursos", UTEC_ADM)
    s2, b2 = api("GET", "cursos", ("NORTE", "admin@inorte.edu.pe"))
    ok(all(c["tenant_id"] == "UTEC" for c in b1["cursos"]), "UTEC solo recibe UTEC")
    # 1) intentar escribir con otro tenant en el cuerpo -> 403 TENANT_MISMATCH
    s, b = api("POST", "cursos", UTEC_DOC, json_body={"tenant_id": "NORTE", "codigo": "HK1", "nombre": "Hack"})
    ok(s == 403 and b["error"] == "TENANT_MISMATCH", "body.tenant_id=NORTE con token UTEC -> 403 TENANT_MISMATCH")
    # 2) tenant en query string
    s, b = api("GET", "cursos", UTEC_DOC, query={"tenant_id": "NORTE"})
    ok(s == 403, "?tenant_id=NORTE con token UTEC -> 403")
    # 3) leer/modificar/borrar por id usando el token de otro tenant -> 404 (no revela existencia)
    nuevo_norte = api("POST", "cursos", NORTE_DOC, json_body={"codigo": "NX1", "nombre": "Curso solo NORTE"})[1]["curso"]["curso_id"] \
        if False else None
    s, b = api("POST", "cursos", NORTE_DOC, json_body={"codigo": "NX1", "nombre": "Curso solo NORTE"})
    ok(s == 201, f"NORTE crea curso -> 201 ({b})")
    cid_norte = b["curso_id"]
    for metodo, path, kw in [("PUT", f"cursos/{cid_norte}", {"json_body": {"nombre": "ROBADO"}}),
                             ("DELETE", f"cursos/{cid_norte}", {}),
                             ("GET", f"cursos/{cid_norte}/modulos", {})]:
        s, b = api(metodo, path, UTEC_ADM, **kw)
        ok(s == 404, f"{metodo} {path} con token UTEC -> {s} (404: ese id no existe en su partición)")
    dn = sql("cursos", "NORTE", "SELECT nombre FROM {s}.cursos WHERE curso_id = %s", (cid_norte,))
    ok(dn and dn[0]["nombre"] == "Curso solo NORTE", "el curso de NORTE quedó intacto")
    ok(not sql("cursos", "UTEC", "SELECT 1 FROM {s}.cursos WHERE curso_id = %s", (cid_norte,)), "y no existe en el esquema de UTEC")
    # 4) token de un alumno no accede a rutas de docente
    s, b = api("GET", "auth/usuarios", UTEC_ANA)
    ok(s == 403 and b["error"] == "ROL_NO_AUTORIZADO", "alumno no puede listar usuarios")
    # 5) cursor de paginación de otro tenant
    s, b = api("GET", "auth/usuarios", UTEC_ADM, query={"limit": "5"})
    cur = b["cursor"]
    ok(s == 200 and cur, "UTEC obtiene cursor")
    s, b = api("GET", "auth/usuarios", ("NORTE", "admin@inorte.edu.pe"), query={"limit": "5", "cursor": cur})
    ok(s == 403 and b["error"] == "CURSOR_AJENO", "cursor de UTEC usado por NORTE -> 403 CURSOR_AJENO")
    # 6) las notificaciones de un usuario no se pueden pedir por id de otro
    s, b = api("POST", "notificaciones/leida", UTEC_ANA, json_body={"id": "A00002#2026-01-01T00:00:00Z#x"})
    ok(s == 403, "marcar como leída una notificación ajena -> 403")
    # 7) los usuarios listados no filtran credenciales
    s, b = api("GET", "auth/usuarios", UTEC_DOC, query={"limit": "50"})
    ok(all("pwd_hash" not in u and "salt" not in u for u in b["usuarios"]), "listar usuarios no expone hash ni salt")
    ok(all(u["tenant_id"] == "UTEC" for u in b["usuarios"]), "y todos son del tenant UTEC")


# ======================================================================= 3. CRUD + paginación
@prueba("CRUD · crear/modificar/eliminar curso visible en RDS; roles; validación")
def t_crud():
    en_db = lambda cid: sql("cursos", "UTEC", "SELECT * FROM {s}.cursos WHERE curso_id = %s", (cid,))
    s, b = api("POST", "cursos", UTEC_ANA, json_body={"codigo": "X1", "nombre": "No"})
    ok(s == 403, "un alumno no crea cursos")
    s, b = api("POST", "cursos", UTEC_DOC, json_body={"codigo": "CS9999"})
    ok(s == 400, "falta `nombre` -> 400")
    s, b = api("POST", "cursos", UTEC_DOC, json_body={"codigo": "CS9999", "nombre": "Arquitectura Cloud", "creditos": 4},)
    ok(s == 201 and b["tenant_id"] == "UTEC" and b["docente_id"] == "D001", "POST /cursos -> 201, tenant y docente salen del token")
    cid = b["curso_id"]
    ok(len(en_db(cid)) == 1, "la fila existe en cursos_utec (esquema del tenant)")
    s, b = api("PUT", f"cursos/{cid}", UTEC_DOC, json_body={"nombre": "Arquitectura Cloud II", "tenant_id": "UTEC", "docente_id": "HACK", "curso_id": "ZZ"})
    ok(s == 200 and b["nombre"] == "Arquitectura Cloud II", "PUT modifica el nombre")
    it = en_db(cid)[0]
    ok(it["docente_id"] == "D001" and it["curso_id"] == cid, "campos fuera de la lista blanca (docente_id, curso_id) se ignoran")
    ok("actualizado_en" in it, "se registra actualizado_en")
    s, b = api("PUT", f"cursos/{cid}", UTEC_DOC, json_body={"zzz": 1})
    ok(s == 400, "PUT sin campos editables -> 400")
    otro = ("UTEC", "admin@utec.edu.pe")
    s, b = api("DELETE", f"cursos/{cid}", otro)
    ok(s == 200 and not en_db(cid), "DELETE por admin -> la fila desaparece de RDS")
    s, b = api("DELETE", f"cursos/{cid}", UTEC_DOC)
    ok(s == 404, "borrar de nuevo -> 404 (nunca crea ni falla en silencio)")
    s, b = api("DELETE", "cursos/C0004", UTEC_DOC)   # curso de otro docente
    ok(s == 403 and en_db("C0004"), "un docente no borra cursos de otro docente")
    s, b = api("PUT", "cursos/C0001", UTEC_DOC, query={"x": "1"}, body_raw="{no es json")
    ok(s == 400 and b["error"], f"JSON inválido -> 400 con código ({b})")
    s, h, b = E.api("PUT", "cursos/C0001", token=tok(*UTEC_DOC), json_body={"nombre": "N"})
    ok(h.get("Access-Control-Allow-Origin") == "*", "las respuestas llevan cabeceras CORS")
    s, h, b = E.api("GET", "cursos", token="Bearer x")
    # (401 del authorizer lo genera API GW: CORS lo cubren los GatewayResponse del yml)
    for s_ in emulador.SERVICIOS[1:5]:
        gr = [r for r in (E.ymls[s_].get("resources", {}).get("Resources") or {}).values() if r["Type"] == "AWS::ApiGateway::GatewayResponse"]
        tipos = {r["Properties"]["ResponseType"] for r in gr}
        ok({"DEFAULT_4XX", "DEFAULT_5XX"} <= tipos, f"{s_}: GatewayResponse CORS en 4XX/5XX")


@prueba("PAGINACIÓN · recorrer 100% de usuarios con cursor, sin repetidos ni pérdidas")
def t_paginacion():
    vistos, cursor, paginas = [], None, 0
    while True:
        q = {"limit": "40"}
        if cursor:
            q["cursor"] = cursor
        s, b = api("GET", "auth/usuarios", UTEC_ADM, query=q)
        ok(s == 200, "página ok")
        vistos += [u["user_id"] for u in b["usuarios"]]
        paginas += 1
        cursor = b["cursor"]
        if not cursor:
            break
        ok(paginas < 50, "termina")
    esperado = sum(1 for (t, _) in E.tabla("sp_usuarios").items if t == "UTEC")
    ok(len(vistos) == len(set(vistos)) == esperado, f"{len(vistos)} usuarios en {paginas} páginas = {esperado} en la tabla")
    s, b = api("GET", "auth/usuarios", UTEC_ADM, query={"limit": "abc"})
    ok(s == 400, "limit no numérico -> 400")
    s, b = api("GET", "auth/usuarios", UTEC_ADM, query={"limit": "5", "cursor": "%%%"})
    ok(s == 400, "cursor corrupto -> 400")
    # query_todo cruza la página de 1000 ítems del emulador (DynamoDB corta a 1 MB)
    t = E.tabla("sp_notificaciones")
    for i in range(1200):
        t.put_item(Item={"tenant_id": "ANDINO", "usuario_ts": f"Z0001#2026-01-01T00:{i // 60:02d}:{i % 60:02d}Z#p{i}",
                         "user_id": "Z0001", "leida": False, "creado_en": f"2026-01-01T{i // 3600:02d}:00:00Z#{i}"})
    mod = E._modulo("ms-notificaciones")
    mod_common = sys.modules["common"]
    todos = mod_common.query_todo(t, maximo=5000, KeyConditionExpression=boto3.dynamodb.conditions.Key("tenant_id").eq("ANDINO")
                                  & boto3.dynamodb.conditions.Key("usuario_ts").begins_with("Z0001#"))
    ok(len(todos) == 1200, f"query_todo recorre las 2 páginas internas ({len(todos)}/1200)")
    for k in [k for k in t.items if k[1].startswith("Z0001#")]:
        del t.items[k]


# ======================================================================= 4. EDA
def tabla_items(nombre, tenant, prefijo_sk=None):
    t = E.tabla(nombre)
    return [it for (h, r), it in t.items.items() if h == tenant and (prefijo_sk is None or r.startswith(prefijo_sk))]


@prueba("EDA · entrega -> nota baja -> semáforo ROJO -> alerta al docente (cadena completa)")
def t_cadena():
    pub0 = len(E.bus.publicados)
    ind = lambda aid: E.tabla("sp_indicadores").items[("UTEC", aid)]
    antes = ind("A00003")
    ok(antes["nivel"] == "AMBAR", f"Valeria parte en AMBAR (seed) → {antes['nivel']} puntaje {antes['puntaje']}")
    # 1) la alumna entrega la tarea 3 (vencida) -> EntregaRegistrada
    s, b = api("POST", "evaluaciones/cursos/C0001/tareas/T0003/entregas", UTEC_VAL, json_body={"texto": "Mi laboratorio, tarde pero aquí"})
    ok(s == 201 and b["es_tardia"] is True, f"POST entrega -> 201 y marcada tardía ({s} {b})")
    s2, _ = api("POST", "evaluaciones/cursos/C0001/tareas/T0003/entregas", UTEC_ANA, json_body={"tenant_id": "NORTE", "texto": "x"})
    ok(s2 == 403, "el alumno no puede entregar en nombre de otro tenant")
    ok(len(E.bus.publicados) == pub0 + 1 and E.bus.publicados[-1]["subject"] == "EntregaRegistrada", "se publicó EntregaRegistrada en SNS")
    ev = E.bus.publicados[-1]
    ok(ev["attrs"]["tenant_id"]["StringValue"] == "UTEC" and ev["attrs"]["evento"]["StringValue"] == "EntregaRegistrada", "con MessageAttributes tenant_id y evento (para filtrar)")
    E.bombear()
    ok(ind("A00003")["n_entregas"] > antes["n_entregas"], "ms-analitica actualizó sus indicadores de forma asíncrona")
    notis = tabla_items("sp_notificaciones", "UTEC", "D001#")
    ok(any(n["evento"] == "EntregaRegistrada" and "Valeria Mendoza" in n["mensaje"] for n in notis), "ms-notificaciones avisó al docente de la entrega")
    # 2) el docente califica con 4 -> NotaPublicada -> riesgo ROJO -> AlumnoEnRiesgo
    s, b = api("PUT", "evaluaciones/cursos/C0001/tareas/T0003/entregas/A00003/calificacion", UTEC_DOC, json_body={"nota": 4, "retroalimentacion": "Hay que reforzar"})
    ok(s == 200 and b["estado"] == "CALIFICADA", "PUT calificación -> 200")
    s, b = api("PUT", "evaluaciones/cursos/C0001/tareas/T0003/entregas/A00003/calificacion", UTEC_VAL, json_body={"nota": 20})
    ok(s == 403 and b["error"] == "ROL_NO_AUTORIZADO", "un alumno no se auto-califica (T-ISO-5)")
    s, b = api("PUT", "evaluaciones/cursos/C0001/tareas/T0003/entregas/A00003/calificacion", UTEC_DOC, json_body={"nota": 25})
    ok(s == 400 and b["error"] == "NOTA_FUERA_DE_RANGO", "nota > puntaje_max -> 400")
    E.bombear()
    despues = ind("A00003")
    ok(despues["nivel"] == "ROJO" and despues["puntaje"] >= 50, f"semáforo AMBAR → {despues['nivel']} ({despues['puntaje']} pts) {despues['motivos']}")
    ok(len(despues["motivos"]) >= 2, "guarda los motivos legibles (R1/R2/R3) para mostrarlos en el panel")
    notis = tabla_items("sp_notificaciones", "UTEC", "D001#")
    riesgo = [n for n in notis if n["evento"] == "AlumnoEnRiesgo" and "Valeria Mendoza" in n["mensaje"] and n["origen"] != "seed"]
    ok(len(riesgo) == 1, "el docente recibe UNA alerta AlumnoEnRiesgo de Valeria")
    nota_al = tabla_items("sp_notificaciones", "UTEC", "A00003#")
    ok(any(n["evento"] == "NotaPublicada" and n["tipo"] == "NOTA" for n in nota_al), "y la alumna recibe su nota publicada")
    # 3) sin bucles: AlumnoEnRiesgo no vuelve a analítica; total de eventos acotado
    eventos = [e["subject"] for e in E.bus.publicados[pub0:]]
    ok(eventos.count("AlumnoEnRiesgo") == 1 and len(eventos) == 3, f"cadena exacta {eventos}")
    ok(all(not m for m in [E.cola("ms-analitica", "ColaAnalitica").msgs, E.cola("ms-notificaciones", "ColaNotificaciones").msgs]), "colas vacías al terminar (sin bucle)")
    # 4) otro tenant intacto
    ok(E.tabla("sp_indicadores").items[("NORTE", "A00003")]["nivel"] == "AMBAR", "Valeria de NORTE sigue AMBAR: el evento fue solo de UTEC")
    ok(not any(n["tenant_id"] != "UTEC" for n in notis), "ninguna notificación de otro tenant en la bandeja del docente")
    # 5) la API de analítica refleja el cambio
    s, b = api("GET", "analitica/riesgo", UTEC_DOC, query={"nivel": "ROJO", "limit": "100"})
    ok(s == 200 and any(x["alumno_id"] == "A00003" for x in b["alumnos"]), f"GET /analitica/riesgo?nivel=ROJO lista a Valeria")
    s, b = api("GET", "analitica/resumen", UTEC_DOC)
    ok(s == 200 and sum(b["conteo"].values()) > 0 if "conteo" in b else s == 200, f"resumen {b}")
    s, b = api("GET", "analitica/alumnos/A00003", UTEC_ANA)
    ok(s == 403, "un alumno no consulta indicadores de otro alumno")
    s, b = api("GET", "analitica/alumnos/A00001", UTEC_ANA)
    ok(s == 200 and b["alumno_id"] == "A00001", "pero sí los propios")
    s, b = api("GET", "analitica/riesgo", UTEC_ANA)
    ok(s == 403, "un alumno no ve el listado de riesgo")
    s, b = api("GET", "notificaciones/traza", UTEC_DOC)
    ok(s == 200 and {"AlumnoEnRiesgo", "NotaPublicada"} <= {e["evento"] for e in b["eventos"]}, "traza de eventos para el widget en vivo")


@prueba("EDA · idempotencia, evento viejo, DLQ y fallo parcial del lote")
def t_resiliencia():
    E.bombear()
    cola = E.cola("ms-notificaciones", "ColaNotificaciones")
    dlq = E.cola("ms-notificaciones", "ColaNotificacionesDLQ")
    base = len(tabla_items("sp_notificaciones", "UTEC"))
    ev = {"event_id": "evt-fijo-1", "evento": "NotaPublicada", "tenant_id": "UTEC", "ts": "2026-11-12T10:00:00Z", "origen": "test",
          "detalle": {"alumno_id": "A00001", "alumno_nombre": "Ana", "curso_id": "C0001", "tarea_id": "T0001", "titulo": "Lab", "nota": 17, "docente_id": "D001", "resumen": {}}}
    sobre = lambda e: json.dumps({"Type": "Notification", "Message": json.dumps(e)})
    for _ in range(3):   # SQS es "al menos una vez": el mismo mensaje puede llegar repetido
        cola.msgs.append({"messageId": "dup-%d" % _, "body": sobre(ev), "rec": 0})
    E.bombear()
    nuevas = len(tabla_items("sp_notificaciones", "UTEC")) - base
    ok(nuevas == 1, f"3 entregas del mismo event_id producen {nuevas} notificación (idempotente)")
    # lote con mensajes buenos y uno venenoso
    venenoso = {"messageId": "veneno", "body": "esto no es json {{{", "rec": 0}
    bueno = {**ev, "event_id": "evt-bueno-2"}
    cola.msgs.extend([{"messageId": "m1", "body": sobre(bueno), "rec": 0}, venenoso])
    E.bombear()
    ok(any(it["event_id"] == "evt-bueno-2" for it in tabla_items("sp_notificaciones", "UTEC")), "el mensaje bueno del lote SÍ se procesó (ReportBatchItemFailures)")
    hist = [h for h in cola.historial if h[0] == "veneno"]
    ok([h[1] for h in hist] == [1, 2, 3] and all(h[2] for h in hist), f"el venenoso se reintentó 3 veces {hist}")
    ok([m["messageId"] for m in dlq.msgs] == ["veneno"], "y terminó en la DLQ (maxReceiveCount=3)")
    ok(not [h for h in cola.historial if h[0] == "m1" and h[2]], "el bueno nunca se reintentó")
    dlq.msgs.clear()
    # evento de analítica atrasado no pisa un estado más nuevo
    ana = E.cola("ms-analitica", "ColaAnalitica")
    ant = dict(E.tabla("sp_indicadores").items[("UTEC", "A00001")])
    viejo = {"event_id": "evt-viejo", "evento": "NotaPublicada", "tenant_id": "UTEC", "ts": "2020-01-01T00:00:00Z", "origen": "test",
             "detalle": {"alumno_id": "A00001", "alumno_nombre": "Ana", "curso_id": "C0001", "tarea_id": "T0001", "nota": 1,
                         "resumen": {"n_entregas": 1, "n_notas": 1, "promedio": 1, "n_tardias": 5, "caida_ultima": 0}}}
    ana.msgs.append({"messageId": "viejo", "body": sobre(viejo), "rec": 0})
    E.bombear()
    ok(E.tabla("sp_indicadores").items[("UTEC", "A00001")]["promedio"] == ant["promedio"], "un evento con ts anterior se descarta (no retrocede el estado)")
    # un evento de otro tenant no puede escribir en el tenant UTEC
    cruz = {**viejo, "event_id": "evt-cruz", "ts": "2030-01-01T00:00:00Z", "tenant_id": "NORTE"}
    ana.msgs.append({"messageId": "cruz", "body": sobre(cruz), "rec": 0})
    E.bombear()
    ok(E.tabla("sp_indicadores").items[("UTEC", "A00001")]["promedio"] == ant["promedio"], "evento NORTE no altera la partición UTEC")
    ok(("NORTE", "A00001") in E.tabla("sp_indicadores").items, "se aplicó en NORTE, su propio tenant")


# ======================================================================= 5. cursos / S3 / práctica
@prueba("S3 · material con URL prefirmada bajo tenant_id=<T>/ y descarga protegida")
def t_s3():
    s, b = api("POST", "cursos/C0001/modulos", UTEC_DOC, json_body={"titulo": "Módulo de prueba"})
    ok(s == 201, f"publicar módulo -> 201 ({b})")
    mid = b["modulo_id"]
    E.bombear()    # ModuloPublicado -> notificaciones y práctica
    s, b = api("POST", f"cursos/C0001/modulos/{mid}/materiales", UTEC_DOC, json_body={"nombre_archivo": "../../etc/pass wd.pdf", "content_type": "application/pdf"})
    ok(s == 201, "POST material -> 201")
    ok(b["material"]["key"].startswith("tenant_id=UTEC/materiales/C0001/"), f"key bajo el prefijo del tenant: {b['material']['key']}")
    ok("/" not in b["material"]["nombre"] and " " not in b["material"]["nombre"], "nombre de archivo saneado (sin '/', sin espacios): no hay path traversal")
    ok(b["upload_url"].startswith("https://sp-storage-123456789012-dev.s3") and "op=put_object" in b["upload_url"], "URL prefirmada PUT hacia el bucket de infra-base")
    s, b2 = api("GET", f"cursos/C0001/modulos/{mid}/materiales/{b['material']['material_id']}/descarga", UTEC_ANA)
    ok(s == 200 and "op=get_object" in b2["url"], "una alumna matriculada obtiene URL de descarga")
    # envenenar: material cuya key apunta al prefijo de otro tenant
    fila = sql("cursos", "UTEC", "SELECT materiales FROM {s}.modulos WHERE curso_id = 'C0001' AND modulo_id = %s", (mid,))[0]
    mats = json.loads(fila["materiales"])
    mats[0]["key"] = "tenant_id=NORTE/materiales/secreto.pdf"
    sql("cursos", "UTEC", "UPDATE {s}.modulos SET materiales = %s WHERE curso_id = 'C0001' AND modulo_id = %s", (json.dumps(mats), mid))
    s, b3 = api("GET", f"cursos/C0001/modulos/{mid}/materiales/{b['material']['material_id']}/descarga", UTEC_ANA)
    ok(s == 403 and b3["error"] == "PREFIJO_AJENO", "defensa en profundidad: prefijo de otro tenant -> 403")
    s, _ = api("POST", f"cursos/C0001/modulos/{mid}/materiales", UTEC_ANA, json_body={"nombre_archivo": "a.pdf"})
    ok(s == 403, "una alumna no sube materiales")
    s, _ = api("POST", f"cursos/C0001/modulos/{mid}/materiales", NORTE_DOC, json_body={"nombre_archivo": "a.pdf"})
    ok(s == 404, "un docente de NORTE no ve el curso de UTEC (404)")
    prac = tabla_items("sp_tarjetas", "UTEC", f"C0001#K{mid}")
    ok(len(prac) == 3, f"ms-practica sembró 3 tarjetas desde ModuloPublicado ({len(prac)}), de forma determinista")
    E.cola("ms-practica", "ColaPractica").msgs.append({"messageId": "r", "rec": 0, "body": json.dumps({"Type": "Notification", "Message": json.dumps(
        {"event_id": "re", "evento": "ModuloPublicado", "tenant_id": "UTEC", "ts": "2026-11-12T00:00:00Z", "detalle": {"curso_id": "C0001", "modulo_id": mid, "titulo": "Módulo de prueba"}})})})
    E.bombear()
    ok(len(tabla_items("sp_tarjetas", "UTEC", f"C0001#K{mid}")) == 3, "reentregar el evento no duplica tarjetas")


@prueba("CURSOS · listado por rol, matrícula (alumno solo a sí mismo) y práctica con QuizCompletado")
def t_cursos_practica():
    s, b = api("GET", "cursos", UTEC_ANA)
    ids = {c["curso_id"] for c in b["cursos"]}
    ok(ids == {"C0001", "C0002", "C0003"}, f"la alumna ve solo sus cursos {ids}")
    s, b = api("GET", "cursos", UTEC_DOC)
    ok({c["curso_id"] for c in b["cursos"]} >= {"C0001", "C0002", "C0003"} and all(c["docente_id"] == "D001" for c in b["cursos"]), "el docente ve sus cursos")
    s, b = api("POST", "cursos/C0004/matriculas", UTEC_ANA, json_body={"alumno_id": "A00002"})
    ok(s == 201 and b["alumno_id"] == "A00001", f"un alumno que pide matricular a OTRO solo se matricula a sí mismo ({b.get('alumno_id')})")
    ok(not sql("cursos", "UTEC", "SELECT 1 FROM {s}.matriculas WHERE curso_id = 'C0004' AND alumno_id = 'A00002'"), "no se creó matrícula ajena")
    s, b = api("POST", "cursos/C0004/matriculas", UTEC_ANA, json_body={})
    ok(s == 409 and b["error"] == "YA_MATRICULADO", "segunda matrícula -> 409 (clave duplicada 1062 controlada)")
    s, b = api("GET", "cursos", UTEC_ANA)
    ok("C0004" in {c["curso_id"] for c in b["cursos"]}, "ahora aparece (JOIN matriculas-cursos por alumno)")
    s, b = api("GET", "cursos/C0001/matriculas", UTEC_DOC)
    ok(s == 200 and len(b["matriculas"]) >= 6, "docente ve las matrículas de su curso")
    s, b = api("GET", "cursos/C0001/matriculas", UTEC_ANA)
    ok(s == 403, "un alumno no ve la lista de matriculados")
    # quiz
    s, b = api("GET", "practica/quiz", UTEC_ANA, query={"curso_id": "C0001"})
    ok(s == 200 and 1 <= len(b["preguntas"]) <= 5, f"quiz con {len(b['preguntas'])} preguntas")
    ok(all("correcta" not in p for p in b["preguntas"]), "la respuesta correcta NUNCA viaja al navegador")
    resp = [{"tarjeta_id": p["tarjeta_id"], "opcion": 0} for p in b["preguntas"]]
    s, b = api("POST", "practica/quiz/completar", UTEC_ANA, json_body={"curso_id": "C0001", "respuestas": resp})
    ok(s == 200 and b["total"] == len(resp) and 0 <= b["aciertos"] <= b["total"], f"calificado en servidor {b['aciertos']}/{b['total']}")
    E.bombear()
    ok(E.bus.publicados[-1]["subject"] == "QuizCompletado", "publica QuizCompletado")
    s, b = api("POST", "practica/quiz/completar", UTEC_ANA, json_body={"curso_id": "C0001", "respuestas": [{"tarjeta_id": "K00001", "opcion": 0}], "tenant_id": "NORTE"})
    ok(s == 403, "quiz con tenant ajeno -> 403")
    s, b = api("GET", "practica/quiz", UTEC_DOC, query={"curso_id": "C0001"})
    ok(s == 403, "el docente no rinde quizzes")


@prueba("EVALUACIONES · crear tarea (id único), listados y mis-notas")
def t_evals():
    ids = set()
    for i in range(5):
        s, b = api("POST", "evaluaciones/cursos/C0001/tareas", UTEC_DOC, json_body={"titulo": f"Nueva {i}", "fecha_limite": "2026-12-20T23:59:00Z"})
        ok(s == 201, f"crear tarea {i}")
        ids.add(b["tarea_id"])
    ok(len(ids) == 5, f"ids únicos aun creándolas seguidas: {sorted(ids)}")
    s, b = api("POST", "evaluaciones/cursos/C0001/tareas", UTEC_DOC, json_body={"titulo": "Sin fecha"})
    ok(s == 400, "falta fecha_limite -> 400")
    s, b = api("POST", "evaluaciones/cursos/C0001/tareas", UTEC_ANA, json_body={"titulo": "x", "fecha_limite": "2026-12-20T23:59:00Z"})
    ok(s == 403, "alumno no crea tareas")
    s, b = api("GET", "evaluaciones/cursos/C0001/tareas", UTEC_ANA)
    ok(s == 200 and len(b["tareas"]) >= 9, f"alumna ve las tareas del curso ({len(b['tareas'])})")
    s, b = api("GET", "evaluaciones/mis-notas", UTEC_ANA)
    ok(s == 200 and b.get("resumen", {}).get("promedio") is not None or "notas" in b, f"mis-notas {list(b)}")
    s, b = api("GET", "evaluaciones/cursos/C0001/tareas/T0001/entregas", UTEC_ANA)
    ok(s == 403, "alumno no lista entregas de la clase")
    s, b = api("GET", "evaluaciones/cursos/C0001/tareas/T0001/entregas", UTEC_DOC)
    ok(s == 200 and len(b["entregas"]) >= 5, f"docente lista entregas ({len(b['entregas'])})")
    s, b = api("GET", "evaluaciones/cursos/C0001/tareas/T0001/entregas", ("NORTE", "profe@inorte.edu.pe"))
    ok(s in (200, 404) and all(e["tenant_id"] == "NORTE" for e in (b.get("entregas") or [])), "NORTE nunca ve entregas de UTEC")


@prueba("RDS · esquema por tenant, CASCADE, inyección SQL, paginación y configuración de infra")
def t_rds():
    import pymysql
    import sqldb
    import yaml
    # 1) un esquema por tenant y por servicio (Pattern 2)
    adj = pymysql.ESTADO["adjuntas"]
    esperados = {f"{sv}_{t}" for sv in ("cursos", "evaluaciones") for t in ("utec", "norte", "andino")}
    ok(esperados <= adj, f"esquemas creados solos al primer uso: {sorted(esperados & adj)}")
    ok(sqldb.esquema("cursos", "UT-EC; DROP TABLE x") == "cursos_utecdroptablex", "el nombre del esquema se sanea (solo a-z, 0-9, _)")
    # 2) los datos de un tenant NO están en el esquema del otro
    ok(not sql("cursos", "NORTE", "SELECT 1 FROM {s}.cursos WHERE curso_id = 'CS9999X'"), "esquema NORTE sin filas de otro tenant")
    # 3) CASCADE: borrar un curso borra sus módulos y matrículas (integridad relacional)
    s_, b = api("POST", "cursos", UTEC_DOC, json_body={"codigo": "RDS1", "nombre": "Curso con hijos"})
    cid = b["curso_id"]
    s_, m = api("POST", f"cursos/{cid}/modulos", UTEC_DOC, json_body={"titulo": "M1"})
    s_, _ = api("POST", f"cursos/{cid}/matriculas", UTEC_ANA, json_body={})
    ok(sql("cursos", "UTEC", "SELECT COUNT(*) AS n FROM {s}.modulos WHERE curso_id = %s", (cid,))[0]["n"] == 1, "hay 1 módulo")
    ok(sql("cursos", "UTEC", "SELECT COUNT(*) AS n FROM {s}.matriculas WHERE curso_id = %s", (cid,))[0]["n"] == 1, "y 1 matrícula")
    s_, _ = api("DELETE", f"cursos/{cid}", UTEC_DOC)
    ok(s_ == 200 and not sql("cursos", "UTEC", "SELECT 1 FROM {s}.modulos WHERE curso_id = %s", (cid,)) and
       not sql("cursos", "UTEC", "SELECT 1 FROM {s}.matriculas WHERE curso_id = %s", (cid,)), "DELETE del curso arrastra módulos y matrículas (ON DELETE CASCADE)")
    # 4) inyección SQL: los valores del usuario viajan SIEMPRE como parámetros
    pymysql.ESTADO["consultas"].clear()
    veneno = "x' OR '1'='1'; DROP TABLE cursos; --"
    for metodo, ruta in [("GET", f"cursos/{veneno}/modulos"), ("PUT", f"cursos/{veneno}"), ("DELETE", f"cursos/{veneno}")]:
        s_, b = api(metodo, ruta, UTEC_ADM, **({"json_body": {"nombre": "n"}} if metodo == "PUT" else {}))
        ok(s_ == 404, f"{metodo} con id malicioso -> 404 ({s_})")
    s_, b = api("POST", "cursos", UTEC_DOC, json_body={"codigo": "INJ", "nombre": "Robert'); DROP TABLE cursos;--"})
    ok(s_ == 201, "un nombre con comillas se guarda como texto")
    ok(sql("cursos", "UTEC", "SELECT nombre FROM {s}.cursos WHERE curso_id = %s", (b["curso_id"],))[0]["nombre"] == "Robert'); DROP TABLE cursos;--", "y se lee idéntico (parametrizado)")
    ok(not any("DROP" in q or "OR '1'" in q for q in pymysql.ESTADO["consultas"]), "ninguna sentencia SQL contiene texto del usuario")
    ok(sql("cursos", "UTEC", "SELECT COUNT(*) AS n FROM {s}.cursos")[0]["n"] > 0, "la tabla cursos sigue existiendo")
    # 5) paginación por cursor (keyset) en matrículas y entregas
    for ruta, clave, campo in [("cursos/C0001/matriculas", "matriculas", "alumno_id"),
                               ("evaluaciones/cursos/C0001/tareas/T0001/entregas", "entregas", "alumno_id")]:
        vistos, cursor, paginas = [], None, 0
        while True:
            q = {"limit": "2"}
            if cursor:
                q["cursor"] = cursor
            s_, b = api("GET", ruta, UTEC_DOC, query=q)
            ok(s_ == 200 and len(b[clave]) <= 2, f"{clave}: página {paginas + 1} ok")
            vistos += [x[campo] for x in b[clave]]
            paginas += 1
            cursor = b["cursor"]
            if not cursor:
                break
            ok(paginas < 400, "termina")
        total = len(sql("cursos" if clave == "matriculas" else "evaluaciones", "UTEC",
                        "SELECT 1 FROM {s}." + ("matriculas WHERE curso_id = 'C0001'" if clave == "matriculas" else "entregas WHERE tarea_id = 'T0001'")))
        ok(len(vistos) == len(set(vistos)) == total and paginas > 1, f"{clave}: {len(vistos)} filas en {paginas} páginas = {total} en la base, sin repetidos")
        s_, b = api("GET", ruta, UTEC_DOC, query={"limit": "2"})
        s2, b2 = api("GET", ruta.replace("C0001", "C0001"), NORTE_DOC, query={"limit": "2", "cursor": b["cursor"]})
        ok(s2 in (403, 404), f"{clave}: el cursor de UTEC no sirve a NORTE ({s2})")
    s_, b = api("GET", "cursos/C0001/matriculas", UTEC_DOC, query={"cursor": "%%%"})
    ok(s_ == 400, "cursor corrupto -> 400")
    # 6) configuración de infraestructura
    infra = yaml.safe_load((RAIZ / "infra-base" / "serverless.yml").read_text(encoding="utf-8"))["resources"]["Resources"]
    bd = next(r for r in infra.values() if r["Type"] == "AWS::RDS::DBInstance")["Properties"]
    ok(bd["Engine"] == "mysql" and bd["MasterUsername"] == "admin" and "${env:DB_PASSWORD}" in bd["MasterUserPassword"], "RDS MySQL con contraseña tomada de la variable de entorno (no del repo)")
    sg = next(r for r in infra.values() if r["Type"] == "AWS::EC2::SecurityGroup")["Properties"]["SecurityGroupIngress"][0]
    ok(sg["FromPort"] == sg["ToPort"] == 3306, "Security Group abre solo el puerto 3306")
    for sv in ("ms-cursos", "ms-evaluaciones"):
        recursos = E.ymls[sv].get("resources", {}).get("Resources") or {}
        ok(not any(r["Type"] == "AWS::DynamoDB::Table" for r in recursos.values()), f"{sv} ya no declara tablas DynamoDB")
        ok({"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD"} <= set(E.envs[sv]) and "T_CURSOS" not in E.envs[sv], f"{sv} recibe DB_HOST/PORT/USER/PASSWORD")
        ok("vendor/**" in E.ymls[sv]["package"]["patterns"], f"{sv} empaqueta el driver (vendor/)")
        ok((RAIZ / sv / "requirements.txt").read_text().startswith("pymysql"), f"{sv}/requirements.txt pide pymysql")
    ok(len({__import__("hashlib").sha256((RAIZ / d / "sqldb.py").read_bytes()).hexdigest() for d in ("ms-cursos", "ms-evaluaciones", "datos-seed")}) == 1, "sqldb.py idéntico en ms-cursos, ms-evaluaciones y datos-seed")


# ======================================================================= runner
def main():
    global E, AHORA
    AHORA = datetime.now(timezone.utc).replace(microsecond=0)
    E = emulador.Emulador()
    ini = time.time()
    global BUF
    BUF = io.StringIO()
    fallos = 0
    for nombre, fn in RESULTADOS:
        try:
            with contextlib.redirect_stdout(BUF if not VERBOSE else sys.stdout):
                fn()
            print(f"  ✓ {nombre}")
        except Exception as e:  # noqa: BLE001
            fallos += 1
            print(f"  ✗ {nombre}\n      {type(e).__name__}: {e}")
            if VERBOSE or not isinstance(e, AssertionError):
                traceback.print_exc()
    print(f"\n{len(RESULTADOS) - fallos}/{len(RESULTADOS)} pruebas OK en {time.time() - ini:.1f}s"
          f"{'' if not fallos else '  <-- HAY FALLOS'}")
    sys.exit(1 if fallos else 0)


if __name__ == "__main__":
    main()
