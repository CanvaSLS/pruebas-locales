# pruebas-locales

Emulador en memoria de DynamoDB, SNS, SQS, S3 y API Gateway que **carga los `serverless.yml` reales** y ejecuta los handlers tal cual.

```bash
python3 test_e2e.py        # 13 pruebas: aislamiento, CRUD, paginación, EDA, DLQ, idempotencia, S3, práctica
python3 test_e2e.py -v     # con detalle
```
Valida lógica, claves/GSIs, flujo de eventos y reglas estrictas de DynamoDB (sin floats, sin Scan). **No** valida IAM/LabRole ni la sintaxis CloudFormation que solo AWS comprueba:
por eso existe `docs-ops/smoke_test.py` para el sistema ya desplegado. Solo necesita Python 3 y PyYAML.

> Los tests leen los microservicios y el seed de las carpetas hermanas: clona **todos** los repos uno junto a otro (`ms-*`, `datos-seed`, `infra-base`, `pruebas-locales`, `docs-ops`).

## RDS en las pruebas
`ms-cursos` y `ms-evaluaciones` usan MySQL real en AWS, pero aquí corren contra `fake_aws/pymysql`, un reemplazo que traduce el SQL a **SQLite en memoria** (esquemas = bases adjuntas, claves foráneas, CASCADE, índices, error 1062 por clave duplicada).
Qué valida: la lógica de los handlers, el SQL parametrizado (hay una prueba de inyección), el aislamiento por esquema, la paginación y el `serverless.yml` de la RDS.
**Qué NO valida:** sintaxis exacta de MySQL, la red (Security Group/VPC), el driver `pymysql` empaquetado ni los permisos. Eso solo se comprueba desplegando en AWS.
