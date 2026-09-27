# Decisiones de diseño

## 1. `flatMap` vs `concatMap`

- **Elegimos:** `concatMap` para reservar los paquetes de un despacho uno por uno.
- **Descartamos:** `flatMap` sin un limite de concurrencia.
- **Porque:** la reserva modifica capacidad compartida y la compensacion necesita registrar con claridad que paquetes ya fueron reservados. El procesamiento secuencial facilita conservar el orden, detenerse ante el primer error y devolver exactamente las reservas completadas.
- **Se rompe si:** un despacho contiene cientos de paquetes y la latencia secuencial resulta demasiado alta. En ese caso se requeriria concurrencia acotada, un registro explicito de reservas exitosas y una compensacion segura bajo concurrencia.

## 2. Estrategia de backpressure del tablero

- **Elegimos:** `onBackpressureLatest`.
- **Descartamos:** un buffer ilimitado o una cola de eventos historicos sin limite.
- **Porque:** el tablero operativo representa el estado mas reciente de los despachos. Para un consumidor lento es mas util recibir la situacion actual que procesar una secuencia atrasada de cambios. La estrategia evita que una conexion lenta consuma memoria indefinidamente.
- **Se rompe si:** el tablero se utiliza como auditoria o trazabilidad historica. En ese escenario no se pueden descartar eventos y seria necesario persistirlos en un almacenamiento consultable.

## 3. Publisher hot frente a publisher cold

- **Elegimos:** `Sinks.many().multicast()` en el bus y `publish().refCount()` para compartir el tablero entre suscriptores.
- **Descartamos:** crear un `Flux` frio independiente para cada cliente del tablero.
- **Porque:** todos los operadores conectados deben observar los eventos del mismo bus sin duplicar consultas, jobs ni fuentes de datos. El multicast distribuye el evento actual a los suscriptores conectados y `refCount` administra la conexion compartida.
- **Se rompe si:** no existe ningun suscriptor en el momento en que se publica un evento. El evento se pierde porque el tablero muestra estado operativo en vivo y no funciona como auditoria historica. Un requerimiento de replay exigiria cambiar la politica a un sink con retencion o persistir los eventos.

## 4. Limite de la transaccion

- **Elegimos:** la transaccion R2DBC cubre la insercion del despacho, la insercion de paquetes y el cambio de `RECIBIDO` a `ASIGNADO`.
- **Descartamos:** mantener la transaccion abierta durante las llamadas HTTP externas o durante toda la reserva de vehiculos.
- **Porque:** las llamadas externas tienen latencia, timeout y fallos independientes; incluirlas en una transaccion retendria conexiones innecesariamente. La capacidad se protege con un `UPDATE` atomico condicionado por `cupo_kg >= peso` y `RETURNING`. Si la persistencia posterior falla, una saga reactiva ejecuta la compensacion.
- **Se rompe si:** el proceso muere despues de reservar capacidad y antes de compensar o completar la persistencia. El job de expiracion y la observabilidad operativa deben detectar y liberar las asignaciones vencidas; para garantizar recuperacion inmediata se requeriria una tabla de reservas o un mecanismo de outbox.

## 5. Compatibilidad de Java

- **Elegimos:** Java 21 como nivel de compilacion y validacion local con JDK 25.
- **Descartamos:** utilizar APIs exclusivas de Java 25 o fijar el codigo a una implementacion concreta del JDK.
- **Porque:** Java 21 proporciona una base estable y compatible con los dos entornos objetivo. El `build.gradle` genera bytecode Java 21 y la compilacion local confirma que el proyecto funciona con el JDK 25 disponible.
- **Se rompe si:** se incorpora una API o una caracteristica cuyo nivel minimo sea posterior a Java 21. En ese caso se debe reemplazar la API o elevar formalmente el nivel minimo del proyecto y validar nuevamente el entorno de ejecucion.