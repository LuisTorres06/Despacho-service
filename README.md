# despacho-service-reactive

Servicio reactivo para gestionar despachos de envios, paquetes, vehiculos, reservas de capacidad, eventos operativos y reportes por ciudad.

El servicio usa Spring WebFlux para HTTP, Spring Data R2DBC para persistencia y Project Reactor para componer operaciones asincronas sin bloquear hilos de servidor.

## Caracteristicas

- Creacion de despachos con validacion declarativa mediante `@Valid`.
- Reserva atomica de capacidad por paquete con PostgreSQL y `UPDATE ... RETURNING`.
- Compensacion reactiva cuando una reserva posterior falla.
- Persistencia transaccional de despacho, paquetes y cambio a `ASIGNADO`.
- Confirmacion de despachos y liberacion de capacidad reservada.
- Idempotencia mediante `Idempotency-Key` para repetir una solicitud sin crear otro despacho.
- Integracion reactiva con tarifa, riesgo y ventana operativa simulados.
- Retry con backoff para tarifa y fallback a una tarifa local.
- Timeout de 800 ms y score de riesgo por defecto.
- Bus de eventos hot mediante `Sinks.Many.multicast()`.
- SSE individual por despacho y tablero operativo global.
- Backpressure del tablero mediante `onBackpressureLatest`.
- Carga masiva de vehiculos mediante NDJSON en lotes de 500.
- Reporte agrupado por ciudad y salida NDJSON acumulada.
- Job periodico de expiracion cada 30 segundos.
- Propagacion de `X-Traza-Id` mediante Reactor Context.
- Respuestas de error con codigo, mensaje, traza e instante.

## Stack y versiones

| Componente | Version o configuracion |
|---|---|
| Java de compilacion | 21 |
| JDK validado localmente | 25.0.4+7 |
| Spring Boot | 4.1.1 |
| Spring WebFlux | Gestionado por Spring Boot |
| Project Reactor | Gestionado por Spring Boot |
| Spring Data R2DBC | Gestionado por Spring Boot |
| PostgreSQL | 15 |
| Driver R2DBC | `org.postgresql:r2dbc-postgresql` |
| Build | Gradle Wrapper 9.1.0 |
| Tests | JUnit 5, Reactor Test, Spring WebFlux Test |

El proyecto usa APIs compatibles con Java 21 y se compila localmente con JDK 25. No se requieren APIs exclusivas de Java 25.

## Arquitectura

```text
HTTP/WebFlux
		|
		+-- Controllers
		|     +-- DespachoController
		|     +-- VehiculoController
		|     +-- ReporteController
		|     +-- TableroController
		|     +-- SimuladorController
		|
		+-- Servicios reactivos
		|     +-- DespachoService
		|     +-- ServiciosExternosClient
		|     +-- ExpiracionDespachosJob
		|     +-- EventBus
		|
		+-- DatabaseClient / R2DBC
					PostgreSQL 15
```

La reserva y la persistencia tienen limites diferentes:

1. Cada paquete intenta reservar capacidad de forma atomica.
2. Las reservas exitosas se registran para poder compensarlas.
3. Cuando terminan las reservas, despacho y paquetes se persisten dentro de `TransactionalOperator`.
4. Si la persistencia falla, se devuelve la capacidad reservada.
5. El despacho queda inicialmente en `RECIBIDO` y luego pasa a `ASIGNADO`.

Las llamadas externas se ejecutan antes de reservar mediante `Mono.zip`, por lo que son independientes y concurrentes. Las llamadas HTTP no se mantienen dentro de la transaccion R2DBC.

## Modelo de negocio

### Vehiculo

| Campo | Descripcion |
|---|---|
| `id` | Identificador del vehiculo |
| `placa` | Placa unica, maximo 10 caracteres |
| `ciudad` | Ciudad operativa |
| `cupo_kg` | Capacidad disponible |
| `reservado_kg` | Capacidad comprometida por despachos asignados |

Reglas:

- `cupo_kg` nunca puede ser negativo.
- `reservado_kg` nunca puede ser negativo.
- Una reserva solo descuenta capacidad si `cupo_kg >= peso`.
- Confirmar un despacho libera su cantidad de `reservado_kg`; el cupo ya fue descontado al reservar.
- Expirar un despacho devuelve el peso a `cupo_kg` y reduce `reservado_kg`.

### Despacho

Estados disponibles:

```text
RECIBIDO -> ASIGNADO -> EN_RUTA -> ENTREGADO
												 |
												 +-- EXPIRADO

RECIBIDO/ASIGNADO -> RECHAZADO
```

Estados terminales para SSE: `ENTREGADO`, `RECHAZADO` y `EXPIRADO`.

Al crear un despacho:

- Se valida el cliente, la ciudad y la lista de paquetes.
- Se calcula el peso total.
- Se consultan tarifa, ventana y riesgo.
- Un riesgo superior a `80` se rechaza con HTTP `422`.
- Se reserva capacidad paquete por paquete usando `concatMap`.
- Se asigna una expiracion de 15 minutos.
- Se publica un evento `ASIGNADO` cuando la persistencia termina correctamente.

La clave de idempotencia se almacena en `despacho.idem_key`. Una solicitud repetida busca el despacho existente y devuelve su representacion.

### Paquete

Cada paquete pertenece a un despacho y referencia un vehiculo. El peso debe ser mayor que cero.

## Base de datos

El esquema se encuentra en `src/main/resources/schema.sql` y crea:

- `vehiculo`
- `despacho`
- `paquete`
- Indices para expiracion y consulta de paquetes.

La inicializacion SQL esta configurada con `spring.sql.init.mode=always`. El esquema usa sentencias idempotentes y agrega tres vehiculos iniciales:

```text
1 - ABC123 - BOG - 500 kg
2 - XYZ987 - MDE - 200 kg
3 - JKL456 - CLO - 800 kg
```

## Configuracion

Configuracion predeterminada en `src/main/resources/application.yml`:

| Propiedad | Valor | Descripcion |
|---|---:|---|
| `spring.r2dbc.url` | `r2dbc:postgresql://localhost:5432/despachodb` | Conexion reactiva |
| `spring.r2dbc.username` | `postgres` | Usuario de base |
| `spring.r2dbc.password` | `postgres` | Puede sobreescribirse con `POSTGRES_PASSWORD` |
| `server.port` | `8081` | Puerto HTTP |
| `app.external.base-url` | `http://localhost:8081` | Base del simulador interno |
| `app.reservation-ttl` | `15m` | Tiempo de expiracion de una asignacion |
| `app.expiry-interval` | `30s` | Frecuencia del job |
| `app.risk-threshold` | `80` | Limite de rechazo |
| `app.default-risk-score` | `30` | Score usado ante timeout |

## Ejecucion en Windows

Desde la raiz del repositorio, seleccionar el JDK portable:

```powershell
$jdkHome = Get-ChildItem .\\.tools\\jdk -Directory | Select-Object -First 1 -ExpandProperty FullName
$env:JAVA_HOME = $jdkHome
$env:Path = "$jdkHome\\bin;$env:Path"
```

Entrar al proyecto:

```powershell
Set-Location .\\despacho-service-reactive
```

Levantar PostgreSQL con Docker:

```powershell
docker compose up -d
```

Compilar, probar e iniciar:

```powershell
.\\gradlew.bat compileJava
.\\gradlew.bat test --no-daemon --console=plain
.\\gradlew.bat bootRun
```

La aplicacion queda disponible en `http://localhost:8081`.

Detener PostgreSQL:

```powershell
docker compose down
```

Tambien puede utilizarse el PostgreSQL portable del repositorio con una base `despachodb` en el puerto `5432`, usuario `postgres` y contraseña `postgres`.

## API HTTP

### Crear despacho

`POST /api/despachos`

Headers opcionales: `X-Traza-Id` e `Idempotency-Key`.

```json
{
	"clienteId": 1001,
	"ciudad": "BOG",
	"paquetes": [
		{ "vehiculoId": 1, "pesoKg": 80 }
	]
}
```

Respuesta exitosa: `201 Created`.

```json
{
	"id": 10,
	"clienteId": 1001,
	"ciudad": "BOG",
	"estado": "ASIGNADO",
	"tarifa": 10800.00,
	"total": 10800.00,
	"scoreRiesgo": 30,
	"trazaId": "trace-001",
	"paquetes": [
		{ "id": 1, "despachoId": 10, "vehiculoId": 1, "pesoKg": 80 }
	]
}
```

### Consultar y confirmar

- `GET /api/despachos/{id}` devuelve el despacho y sus paquetes.
- `POST /api/despachos/{id}/confirm` solo acepta `ASIGNADO`, cambia a `EN_RUTA` y libera `reservado_kg`.

### Eventos SSE

`GET /api/despachos/{id}/events` devuelve `text/event-stream`, filtra por despacho y termina en `ENTREGADO`, `RECHAZADO` o `EXPIRADO`.

`GET /api/ops/tablero` devuelve el flujo global hot compartido. Usa multicast y `onBackpressureLatest` para conservar el evento mas reciente ante consumidores lentos.

Ejemplo de evento:

```json
{
	"despachoId": 10,
	"estado": "ASIGNADO",
	"mensaje": "Despacho asignado",
	"trazaId": "trace-001",
	"instante": "2026-09-22T12:00:00Z"
}
```

### Vehiculos

- `GET /api/vehiculos` lista vehiculos ordenados por identificador.
- `GET /api/vehiculos/{id}` consulta un vehiculo.
- `POST /api/vehiculos` crea o actualiza por `id`.
- `PUT /api/vehiculos/{id}` actualiza un vehiculo existente.
- `DELETE /api/vehiculos/{id}` elimina un vehiculo sin despachos dependientes.
- `POST /api/vehiculos/bulk` acepta `application/x-ndjson`, procesa lotes de 500 y devuelve `{ "procesados": 501 }`.

Ejemplo de vehiculo:

```json
{ "id": 4, "placa": "LMN321", "ciudad": "BOG", "cupoKg": 400 }
```

### Reportes

- `GET /api/reports/ciudades` devuelve kilos y valor agrupados por ciudad.
- `GET /api/reports/ciudades/stream` devuelve `application/x-ndjson` con el acumulado por ciudad.

### Simulador externo

- `GET /external/simulator` consulta el estado.
- `PUT /external/simulator` configura `fallo`, `latenciaMs` y `riesgo`.
- `DELETE /external/simulator` restablece los valores iniciales.

```json
{
	"fallo": 0,
	"latenciaMs": 250,
	"riesgo": 30
}
```

Endpoints internos consumidos por `WebClient`:

- `GET /external/pricing?ciudad=BOG&peso=80`
- `GET /external/risk?ciudad=BOG`
- `GET /external/window?ciudad=BOG`

## Respuestas de error

Formato comun:

```json
{
	"codigo": 409,
	"mensaje": "Cupo insuficiente o vehiculo inexistente",
	"trazaId": "trace-001",
	"instante": "2026-09-22T12:00:00Z"
}
```

| Situacion | HTTP |
|---|---:|
| Cuerpo invalido | 400 |
| Despacho inexistente | 404 |
| Cupo insuficiente o vehiculo inexistente | 409 |
| Estado invalido | 409 |
| Riesgo superior al limite | 422 |

## Reactividad y backpressure

| Elemento | Ubicacion | Uso |
|---|---|---|
| `Mono` / `Flux` | Controllers y servicios | Contratos asincronos y flujos de datos |
| `concatMap` | `DespachoService` | Reservas ordenadas paquete por paquete |
| `Mono.zip` | `DespachoService` | Consultas externas independientes en paralelo |
| `DatabaseClient` + `RETURNING` | `DespachoService` | Reserva atomica de capacidad |
| `TransactionalOperator` | `DatabaseConfig` / `DespachoService` | Persistencia reactiva transaccional |
| `Sinks.many().multicast()` | `EventBus` | Bus hot de eventos |
| `publish().refCount()` | `TableroController` | Stream compartido del tablero |
| `onBackpressureLatest` | `TableroController` | Conserva el estado mas reciente |
| `limitRate` | `ReporteController` | Limita la demanda del reporte |
| `scan` | `ReporteController` | Construye el acumulado NDJSON |
| `Flux.interval` / `onBackpressureDrop` | `ExpiracionDespachosJob` | Job periodico sin solapar pasadas |
| Reactor Context | `TraceWebFilter` | Propagacion de `trazaId` |

## Pruebas

Las pruebas actuales son unitarias y no requieren PostgreSQL. Cubren:

- Bus multicast y ausencia de replay para suscriptores tardios.
- Retry de tarifa ante errores transitorios y ausencia de retry ante `4xx`.
- Fallback de tarifa y timeout de riesgo con tiempo virtual.
- Configuracion, reset y latencia del simulador.
- Reactor Context y generacion de `X-Traza-Id`.
- Recepcion de eventos del tablero.
- Acumulacion del reporte NDJSON.
- Construccion testeable del job de expiracion.
- Contrato basico del controlador de despachos.

Ejecutar la suite:

```powershell
.\\gradlew.bat test --no-daemon --console=plain
```

Las pruebas de flujo completo con PostgreSQL, concurrencia de cupo y carga masiva contra una base real se ejecutan por separado.

## Validacion final del avance

Ultima validacion realizada con el JDK portable `25.0.4+7`:

```text
compileJava: correcto
test: 18 pruebas, 18 exitosas, 0 fallos
pruebas E2E con PostgreSQL: no ejecutadas en esta fase
busqueda de block(), blockFirst(), blockLast(), Thread.sleep, JdbcTemplate y @Transactional en src/main: sin coincidencias
```

La suite actual es deliberadamente independiente de PostgreSQL. Por ello, un resultado verde confirma los flujos unitarios y contratos aislados, pero no confirma todavía la integracion completa con la base de datos.

## Pendientes por completar o validar

### Pendientes obligatorios

- **Version de Java:** el proyecto compila con Java 21 y se valida con JDK 25; el requisito original de entrega solicita Java 17. Debe resolverse esta diferencia antes de cerrar la entrega.
- **Operadores de evidencia:** aun falta incorporar en flujos funcionales reales `flatMapIterable` y `groupBy`. Tambien debe incorporarse `Flux.merge` si se requiere demostrar fusion de fuentes independientes del tablero.
- **Cache de ventana:** `cache(Duration.ofMinutes(10))` esta definido sobre cada `Mono` creado por llamada. Debe revisarse para garantizar una cache compartida por ciudad durante diez minutos.
- **Vehiculo inexistente:** la reserva actual puede devolver `CupoInsuficienteException` para un id inexistente. Debe distinguirse `VehiculoNoExisteException` (404) de cupo insuficiente (409).
- **Trazabilidad:** el filtro escribe `trazaId` en Reactor Context, pero el controller aun lo pasa como parametro al servicio. Debe usarse Context como fuente principal entre capas y verificarse su presencia en logs, errores y eventos.
- **Riesgo alto:** actualmente el riesgo se evalua antes de reservar. Debe validarse que el flujo cumpla la regla de compensacion cuando corresponda y que no queden reservas residuales.

### Validaciones pendientes con PostgreSQL

- Flujo completo `POST /api/despachos` con tres llamadas HTTP, reserva, transaccion y respuesta `201`.
- Compensacion real cuando falla un paquete posterior.
- Invariante `cupo_kg >= 0` bajo 20 solicitudes concurrentes.
- Confirmacion y liberacion real de `reservado_kg`.
- Expiracion real, restitucion de cupo y continuidad del job despues de un error.
- Idempotencia bajo solicitudes concurrentes con la misma clave.
- Pruebas E2E con `WebTestClient` contra el contexto completo.
- Dos clientes SSE recibiendo eventos del tablero.
- Cierre SSE en estados terminales y prueba de contenido de eventos.
- Bulk NDJSON y upsert contra PostgreSQL.
- Reportes SQL con varios paquetes por despacho.

### Pruebas aun no incluidas

- `TestPublisher` para controlar errores tardios y cierre de streams.
- Pruebas de concurrencia de reservas.
- Pruebas E2E y de integracion con PostgreSQL.
- Prueba temporal completa del job mediante `withVirtualTime` y datos simulados.
- Prueba de consumidor lento para demostrar `onBackpressureLatest`.

### Mejoras no obligatorias

- `publishOn(Schedulers.parallel())` para calculos CPU-bound.
- Jitter adicional y mapeo explicito de errores del retry.
- `distinctUntilChanged`, heartbeat y `doOnCancel`/`doFinally` en streams.
- Tabla final de evidencia con elemento reactivo, archivo, linea y proposito.

## Contenido para compartir

El proyecto puede comprimirse excluyendo artefactos generados y recursos locales:

- Excluir `build/` y `.gradle/`.
- Excluir `.tools/`, `target/` y archivos de log.
- Conservar codigo fuente, pruebas, wrapper de Gradle, configuracion, esquema, Docker Compose y documentacion.

El archivo comprimido debe regenerarse despues de cada cambio relevante para que represente exactamente el estado validado.

## Estado tecnico conocido

- La tarifa externa se consulta, se persiste y utiliza el calculo local como fallback.
- El score externo se persiste; el valor `30` se usa ante timeout o fallo.
- El reporte SQL agrega primero los kilos por despacho para evitar multiplicar el valor monetario por la cantidad de paquetes.
- La idempotencia esta respaldada por una restriccion `UNIQUE`; el replay bajo carreras concurrentes requiere validacion con una base real.
- El bus es multicast sin replay historico.
- PostgreSQL es necesario para ejecutar la aplicacion completa porque los servicios usan `DatabaseClient`.

## Estructura principal

```text
src/main/java/com/example/despachoreactive/
	config/       configuracion, transacciones, errores y trazabilidad
	controller/   endpoints HTTP, SSE, reportes y simulador
	dto/          requests, responses y eventos
	service/      flujo de despacho, integraciones, bus y expiracion
src/main/resources/
	application.yml
	schema.sql
src/test/java/com/example/despachoreactive/
	pruebas unitarias reactivas y de contratos HTTP
```
