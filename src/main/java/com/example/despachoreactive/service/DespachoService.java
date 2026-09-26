package com.example.despachoreactive.service;

import com.example.despachoreactive.config.TraceWebFilter;
import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.dto.DespachoRequest;
import com.example.despachoreactive.dto.DespachoResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class DespachoService {
    private static final String INSERT_RECIBIDO = """
            INSERT INTO despacho (cliente_id, ciudad, estado, traza_id, idem_key)
            VALUES (:clienteId, :ciudad, 'RECIBIDO', :trazaId, :idemKey)
            RETURNING id
            """;

    private static final String RESERVE = """
            UPDATE vehiculo
            SET cupo_kg = cupo_kg - :peso,
                reservado_kg = reservado_kg + :peso
            WHERE id = :vehiculoId
              AND cupo_kg >= :peso
            RETURNING id
            """;

    private static final String RELEASE = """
            UPDATE vehiculo
            SET cupo_kg = cupo_kg + :peso,
                reservado_kg = GREATEST(reservado_kg - :peso, 0)
            WHERE id = :vehiculoId
            """;

    private static final String UPDATE_ASIGNADO = """
            UPDATE despacho
            SET estado = 'ASIGNADO',
                tarifa = :tarifa,
                total = :total,
                score_riesgo = :score,
                expira_en = :expiraEn
            WHERE id = :id
            """;

    private static final String INSERT_PAQUETE = """
            INSERT INTO paquete (despacho_id, vehiculo_id, peso_kg)
            VALUES (:despachoId, :vehiculoId, :peso)
            """;

    private static final String SELECT_VEHICULO = "SELECT id FROM vehiculo WHERE id = :id";
    private static final String SELECT_BY_IDEM = "SELECT id FROM despacho WHERE idem_key = :clave";
    private static final String UPDATE_ESTADO = "UPDATE despacho SET estado = :estado WHERE id = :id";

    private final DatabaseClient db;
    private final TransactionalOperator tx;
    private final EventBus bus;
    private final ServiciosExternosClient externos;
    private final Duration reservationTtl;
    private final int riskThreshold;

    public DespachoService(
            DatabaseClient db,
            TransactionalOperator tx,
            EventBus bus,
            ServiciosExternosClient externos,
            @Value("${app.reservation-ttl:15m}") Duration reservationTtl,
            @Value("${app.risk-threshold:80}") int riskThreshold
    ) {
        this.db = db;
        this.tx = tx;
        this.bus = bus;
        this.externos = externos;
        this.reservationTtl = reservationTtl;
        this.riskThreshold = riskThreshold;
    }

    public Mono<DespachoResponse> crear(DespachoRequest request, String idemKey) {
        return Mono.deferContextual(context -> {
            String trazaId = context.getOrDefault(TraceWebFilter.KEY, "n/a");
            String normalizedIdemKey = (idemKey == null || idemKey.isBlank()) ? null : idemKey.trim();

            Mono<DespachoResponse> existente = normalizedIdemKey == null
                    ? Mono.empty()
                    : buscarPorClave(normalizedIdemKey);

            Mono<DespachoResponse> nuevo = crearNuevo(request, trazaId, normalizedIdemKey);
            if (normalizedIdemKey != null) {
                nuevo = nuevo.onErrorResume(
                        DataIntegrityViolationException.class,
                        error -> buscarPorClave(normalizedIdemKey)
                );
            }

            return existente.switchIfEmpty(nuevo);
        });
    }

    private Mono<DespachoResponse> crearNuevo(DespachoRequest request, String trazaId, String idemKey) {
        int pesoTotal = request.paquetes().stream().mapToInt(DespachoRequest.PaqueteRequest::pesoKg).sum();
        BigDecimal tarifaFallback = BigDecimal.valueOf(10000L + (long) pesoTotal * 10L);
        List<DespachoRequest.PaqueteRequest> reservados = new ArrayList<>();

        return insertarRecibido(request, trazaId, idemKey)
                .flatMap(despachoId ->
                        Flux.fromIterable(request.paquetes())
                                .concatMap(paquete -> reservar(paquete).doOnNext(ok -> reservados.add(paquete)))
                                .then(Mono.zip(
                                        externos.tarifa(request.ciudad(), pesoTotal, tarifaFallback),
                                        externos.ventana(request.ciudad()),
                                        externos.riesgo(request.ciudad())
                                ))
                                .flatMap(datos -> {
                                    BigDecimal tarifa = datos.getT1();
                                    Integer scoreRiesgo = datos.getT3();

                                    if (scoreRiesgo > riskThreshold) {
                                        return Mono.error(new ZonaRiesgosaException(scoreRiesgo));
                                    }

                                    Instant expiraEn = Instant.now().plus(reservationTtl);
                                    return tx.transactional(
                                            guardarPaquetes(despachoId, request.paquetes())
                                                    .then(actualizarAsignado(despachoId, tarifa, scoreRiesgo, expiraEn))
                                    );
                                })
                                .onErrorResume(error ->
                                        compensarError(despachoId, reservados, trazaId, error).then(Mono.error(error))
                                )
                                .then(cargar(despachoId))
                                .doOnSuccess(resultado ->
                                        bus.publicar(new DespachoEvent(
                                                resultado.id(),
                                                resultado.estado(),
                                                "Despacho asignado",
                                                resultado.trazaId(),
                                                Instant.now()
                                        ))
                                )
                );
    }

    private Mono<Long> insertarRecibido(DespachoRequest request, String trazaId, String idemKey) {
        DatabaseClient.GenericExecuteSpec spec = db.sql(INSERT_RECIBIDO)
                .bind("clienteId", request.clienteId())
                .bind("ciudad", request.ciudad())
                .bind("trazaId", trazaId);

        spec = idemKey == null
                ? spec.bindNull("idemKey", String.class)
                : spec.bind("idemKey", idemKey);

        return spec.map((row, metadata) -> row.get("id", Long.class)).one();
    }

    private Mono<Long> reservar(DespachoRequest.PaqueteRequest paquete) {
        return db.sql(RESERVE)
                .bind("vehiculoId", paquete.vehiculoId())
                .bind("peso", paquete.pesoKg())
                .map((row, metadata) -> row.get("id", Long.class))
                .one()
                .switchIfEmpty(verificarExistenciaYFallar(paquete.vehiculoId(), paquete.pesoKg()));
    }

    private Mono<Long> verificarExistenciaYFallar(Long vehiculoId, int pesoKg) {
        return db.sql(SELECT_VEHICULO)
                .bind("id", vehiculoId)
                .map((row, metadata) -> row.get("id", Long.class))
                .one()
                .switchIfEmpty(Mono.error(new VehiculoNoExisteException(vehiculoId)))
                .flatMap(id -> Mono.error(new CupoInsuficienteException(vehiculoId, pesoKg)));
    }

    private Mono<Void> liberar(List<DespachoRequest.PaqueteRequest> paquetes) {
        return Flux.fromIterable(paquetes)
                .concatMap(paquete -> db.sql(RELEASE)
                        .bind("vehiculoId", paquete.vehiculoId())
                        .bind("peso", paquete.pesoKg())
                        .fetch()
                        .rowsUpdated())
                .then();
    }

    private Mono<Void> compensarError(
            Long despachoId,
            List<DespachoRequest.PaqueteRequest> reservados,
            String trazaId,
            Throwable error
    ) {
        Mono<Void> compensacion = liberar(reservados);
        if (error instanceof ZonaRiesgosaException) {
            compensacion = compensacion
                    .then(cambiarEstado(despachoId, "RECHAZADO"))
                    .doOnSuccess(rows -> bus.publicar(new DespachoEvent(
                            despachoId,
                            "RECHAZADO",
                            "Despacho rechazado por riesgo",
                            trazaId,
                            Instant.now()
                    )))
                    .then();
        }
        return compensacion;
    }

    private Mono<Void> guardarPaquetes(Long despachoId, List<DespachoRequest.PaqueteRequest> paquetes) {
        return Flux.fromIterable(paquetes)
                .concatMap(paquete -> db.sql(INSERT_PAQUETE)
                        .bind("despachoId", despachoId)
                        .bind("vehiculoId", paquete.vehiculoId())
                        .bind("peso", paquete.pesoKg())
                        .fetch()
                        .rowsUpdated())
                .then();
    }

    private Mono<Long> actualizarAsignado(Long id, BigDecimal tarifa, Integer scoreRiesgo, Instant expiraEn) {
        return db.sql(UPDATE_ASIGNADO)
                .bind("id", id)
                .bind("tarifa", tarifa)
                .bind("total", tarifa)
                .bind("score", scoreRiesgo)
                .bind("expiraEn", expiraEn)
                .fetch()
                .rowsUpdated();
    }

    public Mono<DespachoResponse> buscar(Long id) {
        return cargar(id).switchIfEmpty(Mono.error(new DespachoNoExisteException(id)));
    }

    private Mono<DespachoResponse> buscarPorClave(String clave) {
        return db.sql(SELECT_BY_IDEM)
                .bind("clave", clave)
                .map((row, metadata) -> row.get("id", Long.class))
                .one()
                .flatMap(this::cargar);
    }

    private Mono<DespachoResponse> cargar(Long id) {
        return db.sql("""
                        SELECT id, cliente_id, ciudad, estado, tarifa, total, score_riesgo, traza_id, creado_en, expira_en
                        FROM despacho
                        WHERE id = :id
                        """)
                .bind("id", id)
                .map((row, metadata) -> new DespachoResponse(
                        row.get("id", Long.class),
                        row.get("cliente_id", Long.class),
                        row.get("ciudad", String.class),
                        row.get("estado", String.class),
                        row.get("tarifa", BigDecimal.class),
                        row.get("total", BigDecimal.class),
                        row.get("score_riesgo", Integer.class),
                        row.get("traza_id", String.class),
                        row.get("creado_en", Instant.class),
                        row.get("expira_en", Instant.class),
                        List.of()
                ))
                .one()
                .flatMap(despacho -> db.sql("""
                                SELECT id, despacho_id, vehiculo_id, peso_kg
                                FROM paquete
                                WHERE despacho_id = :id
                                ORDER BY id
                                """)
                        .bind("id", id)
                        .map((row, metadata) -> new DespachoResponse.PaqueteResponse(
                                row.get("id", Long.class),
                                row.get("despacho_id", Long.class),
                                row.get("vehiculo_id", Long.class),
                                row.get("peso_kg", Integer.class)
                        ))
                        .all()
                        .collectList()
                        .map(paquetes -> new DespachoResponse(
                                despacho.id(),
                                despacho.clienteId(),
                                despacho.ciudad(),
                                despacho.estado(),
                                despacho.tarifa(),
                                despacho.total(),
                                despacho.scoreRiesgo(),
                                despacho.trazaId(),
                                despacho.creadoEn(),
                                despacho.expiraEn(),
                                paquetes
                        )));
    }

    public Mono<DespachoResponse> confirmar(Long id) {
        return buscar(id).flatMap(despacho -> {
            if (!"ASIGNADO".equals(despacho.estado())) {
                return Mono.error(new EstadoInvalidoException(despacho.estado(), "ASIGNADO"));
            }

            Mono<Long> confirmacion = tx.transactional(
                    db.sql("SELECT vehiculo_id, peso_kg FROM paquete WHERE despacho_id = :id")
                            .bind("id", id)
                            .map((row, metadata) -> new DespachoRequest.PaqueteRequest(
                                    row.get("vehiculo_id", Long.class),
                                    row.get("peso_kg", Integer.class)
                            ))
                            .all()
                            .concatMap(paquete -> db.sql("""
                                            UPDATE vehiculo
                                            SET reservado_kg = GREATEST(reservado_kg - :peso, 0)
                                            WHERE id = :vehiculoId
                                            """)
                                    .bind("peso", paquete.pesoKg())
                                    .bind("vehiculoId", paquete.vehiculoId())
                                    .fetch()
                                    .rowsUpdated())
                            .then(cambiarEstado(id, "EN_RUTA"))
            );

            return confirmacion
                    .then(cargar(id))
                    .doOnSuccess(resultado -> bus.publicar(new DespachoEvent(
                            resultado.id(),
                            resultado.estado(),
                            "Despacho confirmado",
                            resultado.trazaId(),
                            Instant.now()
                    )));
        });
    }

    private Mono<Long> cambiarEstado(Long id, String estado) {
        return db.sql(UPDATE_ESTADO)
                .bind("id", id)
                .bind("estado", estado)
                .fetch()
                .rowsUpdated();
    }
}