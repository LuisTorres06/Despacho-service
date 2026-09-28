# Despacho Service Reactive

Servicio reactivo para gestionar despachos, reservas de cupo en vehiculos, eventos operativos SSE, reportes por ciudad y carga masiva NDJSON.

# Nota:
Para ejecutar el proyecto localmente, se hicieron unos cambios:
1. En build.gradle, se cambió repositories, esto con el fin de que el proyecto pueda descargar las dependencias desde el repositorio de Bancolombia.

```
repositories {
    maven { url "https://artifactory.apps.bancolombia.com:443/maven-bancolombia" }
}
```

tambien se agregó a settings.gradle el siguiente bloque de código para que el plugin de Spring Boot pueda descargar las dependencias desde el repositorio de Bancolombia.

```
pluginManagement {
    repositories {
        maven {
            url "https://artifactory.apps.bancolombia.com/maven-bancolombia"
        }
    }
}
```
Para ejecutar el proyecto en una maquina no banco se recomienda cambiar el maven para que apunte a repocentral y el eliminar el bloque de pluginManagement de settings.gradle.

2. Tampoco se utilizó docker para levantar la base de datos, se utilizó directamente la base de datos local postgres 15.


## Stack tecnico  

- Java 21
- Spring Boot 4.1.1
- Spring WebFlux
- Spring Data R2DBC
- PostgreSQL 15
- Project Reactor
- Gradle Wrapper

## Requisitos

- Docker Desktop -> se usó para levantar PostgreSQL 15 con R2DBC
- JDK 21
- Puerto 5432 libre para PostgreSQL
- Puerto 8081 libre para la app

## Levantar el proyecto

### 1) Base de datos

~~~bash
docker compose up -d
~~~

### 2) Build y tests

**Windows (PowerShell):**
~~~powershell
.\gradlew.bat build
.\gradlew.bat test
~~~

**macOS/Linux:**
~~~bash
./gradlew build
./gradlew test
~~~

### 3) Ejecutar app

**Windows:**
~~~powershell
.\gradlew.bat bootRun
~~~

**macOS/Linux:**
~~~bash
./gradlew bootRun
~~~

La aplicacion queda en:

- `http://localhost:8081`

---

## Flujo de negocio principal (`POST /api/despachos`)

1. Validar request (`@Valid`).
2. Crear despacho en estado `RECIBIDO`.
3. Reservar cupo paquete por paquete con `UPDATE ... WHERE cupo_kg >= :peso RETURNING`.
4. Consultar externos en paralelo con `Mono.zip`: tarifa, ventana, riesgo.
5. Si el riesgo supera el umbral, compensar reservas y rechazar con 422.
6. Guardar paquetes y actualizar a `ASIGNADO` + `expira_en` dentro de transaccion reactiva.
7. Emitir evento al bus interno (`Sinks.many().multicast()`).

---

## Endpoints

| Metodo | Ruta | Descripcion | Tipo |
|---|---|---|---|
| POST | `/api/despachos` | Crea despacho (headers: `X-Traza-Id`, `Idempotency-Key`) | JSON |
| GET | `/api/despachos/{id}` | Consulta despacho con paquetes | JSON |
| POST | `/api/despachos/{id}/confirm` | Transicion `ASIGNADO -> EN_RUTA` | JSON |
| GET | `/api/despachos/{id}/events` | SSE por despacho, cierra en estado terminal | `text/event-stream` |
| GET | `/api/ops/tablero` | SSE global compartido (hot) | `text/event-stream` |
| GET | `/api/reports/ciudades` | Totales por ciudad | JSON |
| GET | `/api/reports/ciudades/stream` | Acumulado en vivo | `application/x-ndjson` |
| POST | `/api/vehiculos/bulk` | Carga NDJSON por lotes | `application/x-ndjson` |
| GET | `/external/simulator` | Estado del simulador | JSON |
| PUT | `/external/simulator` | Configura fallos/latencias/riesgo | JSON |
| DELETE | `/external/simulator` | Resetea simulador | JSON |

### Endpoints internos consumidos por WebClient

- `GET /external/pricing?ciudad=BOG&peso=120`
- `GET /external/risk?ciudad=BOG`
- `GET /external/window?ciudad=BOG`

---

## Simulador externo (contrato de taller)

`PUT /external/simulator`

~~~json
{
  "fallasTarifa": 2,
  "latenciaRiesgoMs": 3000,
  "scoreRiesgo": 95
}
~~~

- `fallasTarifa`: cantidad de fallos transitorios de tarifa antes de volver a responder normal.
- `latenciaRiesgoMs`: latencia artificial en riesgo.
- `scoreRiesgo`: valor devuelto por riesgo.

Reset:

~~~bash
curl -X DELETE http://localhost:8081/external/simulator
~~~

---

## Ejemplos rapidos

### Crear despacho

~~~json
{
  "clienteId": 1,
  "ciudad": "BOG",
  "paquetes": [
    { "vehiculoId": 1, "pesoKg": 120 }
  ]
}
~~~

### Bulk NDJSON de vehiculos

~~~ndjson
{"id":1,"placa":"ABC123","ciudad":"BOG","cupoKg":500}
{"id":2,"placa":"XYZ987","ciudad":"MDE","cupoKg":200}
~~~

---

## Manejo de errores

Cuerpo uniforme:

~~~json
{
  "codigo": 409,
  "mensaje": "Cupo insuficiente para vehiculo 1 y peso 120",
  "trazaId": "trace-1",
  "instante": "2026-09-25T21:00:00Z"
}
~~~

Errores de dominio principales:

- `VehiculoNoExisteException` -> 404
- `CupoInsuficienteException` -> 409
- `ZonaRiesgosaException` -> 422
- `DespachoNoExisteException` -> 404
- `EstadoInvalidoException` -> 409
- Validacion (`WebExchangeBindException`) -> 400

---

## Tabla de evidencia reactiva (elemento -> archivo:linea)


| Elemento reactivo | Archivo:linea | Uso |
|---|---|---|
| `Mono.deferContextual` | `src/main/java/com/example/despachoreactive/service/DespachoService.java:88` | Lee `trazaId` desde Reactor Context |
| `contextWrite` | `src/main/java/com/example/despachoreactive/config/TraceWebFilter.java:19` | Inyecta `trazaId` al contexto |
| `deferContextual` en errores | `src/main/java/com/example/despachoreactive/config/GlobalErrorHandler.java:18,23` | Incluye `trazaId` en respuesta de error |
| Reserva atomica `UPDATE ... RETURNING` | `src/main/java/com/example/despachoreactive/service/DespachoService.java:30-35` | Evita cupo negativo por carrera |
| `concatMap` (reserva secuencial) | `src/main/java/com/example/despachoreactive/service/DespachoService.java:116` | Reserva paquete por paquete |
| `Mono.zip` externos en paralelo | `src/main/java/com/example/despachoreactive/service/DespachoService.java:117` | Tarifa + ventana + riesgo concurrentes |
| `TransactionalOperator` | `src/main/java/com/example/despachoreactive/service/DespachoService.java:131,306` | Persistencia reactiva transaccional |
| `retryWhen(Retry.backoff)` | `src/main/java/com/example/despachoreactive/service/ServiciosExternosClient.java:46` | Reintento de tarifa transitoria |
| `timeout` riesgo | `src/main/java/com/example/despachoreactive/service/ServiciosExternosClient.java:58` | Proteccion ante cuelgue |
| `cache(Duration)` ventana | `src/main/java/com/example/despachoreactive/service/ServiciosExternosClient.java:70` | Cache temporal por ciudad |
| `Sinks.many().multicast()` | `src/main/java/com/example/despachoreactive/service/EventBus.java:10` | Bus hot interno |
| `publish().refCount(1)` | `src/main/java/com/example/despachoreactive/controller/TableroController.java:22` | Stream global compartido |
| `onBackpressureLatest()` | `src/main/java/com/example/despachoreactive/controller/TableroController.java:22` | Backpressure del tablero |
| `onBackpressureDrop()` | `src/main/java/com/example/despachoreactive/service/ExpiracionDespachosJob.java:47` | Evita cola infinita de ticks |
| `limitRate(100)` | `src/main/java/com/example/despachoreactive/controller/ReporteController.java:29` | Control de demanda en reporte |
| `scan(...)` | `src/main/java/com/example/despachoreactive/controller/ReporteController.java:34` | Acumulado NDJSON en vivo |
| SSE por despacho | `src/main/java/com/example/despachoreactive/controller/DespachoController.java:49` | Stream por despacho |
| SSE tablero | `src/main/java/com/example/despachoreactive/controller/TableroController.java:20` | Stream ops global |
| NDJSON salida | `src/main/java/com/example/despachoreactive/controller/ReporteController.java:32` | Streaming de reportes |
| NDJSON entrada | `src/main/java/com/example/despachoreactive/controller/VehiculoController.java:51` | Carga masiva por lotes |
| Simulador controlable | `src/main/java/com/example/despachoreactive/controller/SimuladorController.java:14,20,29,54` | GET/PUT/DELETE de simulador |

---

## Pruebas

Ejecutar:

**Windows**
~~~powershell
.\gradlew.bat test
~~~

**macOS/Linux**
~~~bash
./gradlew test
~~~

Pruebas relevantes del taller:

- `DespachoE2ETest`
- `CupoConcurrenciaTest`
- `SseEndpointsTest`
- `VehiculoBulkNdjsonTest`
- `ReporteStreamNdjsonTest`
- `ServiciosExternosClientTest`
- `TableroControllerTestPublisherTest`
- `TraceWebFilterTest`

---