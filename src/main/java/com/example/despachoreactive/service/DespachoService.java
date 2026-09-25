package com.example.despachoreactive.service;

import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.dto.DespachoRequest;
import com.example.despachoreactive.dto.DespachoResponse;
import org.springframework.http.HttpStatus;
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
    private static final String INSERT = "INSERT INTO despacho (cliente_id, ciudad, estado, tarifa, total, score_riesgo, traza_id, idem_key, expira_en) VALUES (:clienteId, :ciudad, 'RECIBIDO', :tarifa, :total, :score, :trazaId, :idemKey, :expiraEn) RETURNING id, cliente_id, ciudad, estado, tarifa, total, score_riesgo, traza_id, creado_en, expira_en";
    private static final String RESERVE = "UPDATE vehiculo SET cupo_kg = cupo_kg - :peso, reservado_kg = reservado_kg + :peso WHERE id = :vehiculoId AND cupo_kg >= :peso RETURNING id";
    private static final String RELEASE = "UPDATE vehiculo SET cupo_kg = cupo_kg + :peso, reservado_kg = GREATEST(reservado_kg - :peso, 0) WHERE id = :vehiculoId";

    private final DatabaseClient db;
    private final TransactionalOperator tx;
    private final EventBus bus;
    private final ServiciosExternosClient externos;

    public DespachoService(DatabaseClient db, TransactionalOperator tx, EventBus bus, ServiciosExternosClient externos) {
        this.db = db;
        this.tx = tx;
        this.bus = bus;
        this.externos = externos;
    }

    public Mono<DespachoResponse> crear(DespachoRequest request, String trazaId, String idemKey) {
        int peso = request.paquetes().stream().mapToInt(DespachoRequest.PaqueteRequest::pesoKg).sum();
        BigDecimal fallback = BigDecimal.valueOf(10000L + peso * 10L);
        return Mono.zip(externos.tarifa(request.ciudad(), peso, fallback), externos.ventana(request.ciudad()), externos.riesgo(request.ciudad()))
                .flatMap(datos -> {
                    if (datos.getT3() > 80) return Mono.error(new DomainException("Zona riesgosa", HttpStatus.UNPROCESSABLE_ENTITY));
                    return idemKey == null || idemKey.isBlank() ? crearNuevo(request, trazaId, null, datos.getT1(), datos.getT3()) : buscarPorClave(idemKey).switchIfEmpty(Mono.defer(() -> crearNuevo(request, trazaId, idemKey, datos.getT1(), datos.getT3())));
                });
    }

    private Mono<DespachoResponse> crearNuevo(DespachoRequest request, String trazaId, String idemKey, BigDecimal tarifa, Integer scoreRiesgo) {
        Instant expira = Instant.now().plus(Duration.ofMinutes(15));
        List<DespachoRequest.PaqueteRequest> reservados = new ArrayList<>();
        Mono<DespachoResponse> persistencia = Flux.fromIterable(request.paquetes())
            .concatMap(paquete -> reservar(paquete).doOnNext(ignorado -> reservados.add(paquete)))
            .then(Mono.defer(() -> tx.transactional(db.sql(INSERT)
                .bind("clienteId", request.clienteId()).bind("ciudad", request.ciudad())
                .bind("tarifa", tarifa).bind("total", tarifa).bind("score", scoreRiesgo)
                .bind("trazaId", trazaId).bind("idemKey", idemKey).bind("expiraEn", expira)
                .map((row, metadata) -> new DespachoResponse(row.get("id", Long.class), row.get("cliente_id", Long.class), row.get("ciudad", String.class), row.get("estado", String.class), row.get("tarifa", BigDecimal.class), row.get("total", BigDecimal.class), row.get("score_riesgo", Integer.class), row.get("traza_id", String.class), row.get("creado_en", Instant.class), row.get("expira_en", Instant.class), List.of()))
                .one()
                .flatMap(despacho -> guardarPaquetes(despacho.id(), request.paquetes()).then(cambiarEstado(despacho.id(), "ASIGNADO")).then(cargar(despacho.id()))))))
            .doOnSuccess(resultado -> bus.publicar(new DespachoEvent(resultado.id(), resultado.estado(), "Despacho asignado", trazaId, Instant.now())))
            .onErrorResume(error -> liberar(reservados).then(Mono.error(error)));
        return persistencia;
    }

    private Mono<Long> reservar(DespachoRequest.PaqueteRequest paquete) {
        return db.sql(RESERVE).bind("vehiculoId", paquete.vehiculoId()).bind("peso", paquete.pesoKg()).map((row, metadata) -> row.get("id", Long.class)).one().switchIfEmpty(Mono.error(new CupoInsuficienteException(paquete.vehiculoId(), paquete.pesoKg())));
    }

    private Mono<Void> liberar(List<DespachoRequest.PaqueteRequest> paquetes) {
        return Flux.fromIterable(paquetes).concatMap(p -> db.sql(RELEASE).bind("vehiculoId", p.vehiculoId()).bind("peso", p.pesoKg()).fetch().rowsUpdated()).then();
    }

    private Mono<Void> guardarPaquetes(Long despachoId, List<DespachoRequest.PaqueteRequest> paquetes) {
        return Flux.fromIterable(paquetes).concatMap(p -> db.sql("INSERT INTO paquete (despacho_id, vehiculo_id, peso_kg) VALUES (:despachoId, :vehiculoId, :peso)").bind("despachoId", despachoId).bind("vehiculoId", p.vehiculoId()).bind("peso", p.pesoKg()).fetch().rowsUpdated()).then();
    }

    public Mono<DespachoResponse> buscar(Long id) {
        return cargar(id).switchIfEmpty(Mono.error(new DomainException("Despacho no existe", HttpStatus.NOT_FOUND)));
    }

    private Mono<DespachoResponse> buscarPorClave(String clave) {
        return db.sql("SELECT id FROM despacho WHERE idem_key = :clave").bind("clave", clave).map((r, m) -> r.get("id", Long.class)).one().flatMap(this::cargar);
    }

    private Mono<DespachoResponse> cargar(Long id) {
        return db.sql("SELECT id, cliente_id, ciudad, estado, tarifa, total, score_riesgo, traza_id, creado_en, expira_en FROM despacho WHERE id = :id").bind("id", id).map((r, m) -> new DespachoResponse(r.get("id", Long.class), r.get("cliente_id", Long.class), r.get("ciudad", String.class), r.get("estado", String.class), r.get("tarifa", BigDecimal.class), r.get("total", BigDecimal.class), r.get("score_riesgo", Integer.class), r.get("traza_id", String.class), r.get("creado_en", Instant.class), r.get("expira_en", Instant.class), List.of())).one().flatMap(d -> db.sql("SELECT id, despacho_id, vehiculo_id, peso_kg FROM paquete WHERE despacho_id = :id ORDER BY id").bind("id", id).map((r, m) -> new DespachoResponse.PaqueteResponse(r.get("id", Long.class), r.get("despacho_id", Long.class), r.get("vehiculo_id", Long.class), r.get("peso_kg", Integer.class))).all().collectList().map(p -> new DespachoResponse(d.id(), d.clienteId(), d.ciudad(), d.estado(), d.tarifa(), d.total(), d.scoreRiesgo(), d.trazaId(), d.creadoEn(), d.expiraEn(), p)));
    }

    public Mono<DespachoResponse> confirmar(Long id) {
        return buscar(id).flatMap(d -> {
            if (!"ASIGNADO".equals(d.estado())) return Mono.error(new DomainException("Estado invalido", HttpStatus.CONFLICT));
                return db.sql("SELECT vehiculo_id, peso_kg FROM paquete WHERE despacho_id = :id").bind("id", id).map((r, m) -> new DespachoRequest.PaqueteRequest(r.get("vehiculo_id", Long.class), r.get("peso_kg", Integer.class))).all()
                    .concatMap(p -> db.sql("UPDATE vehiculo SET reservado_kg = GREATEST(reservado_kg - :peso, 0) WHERE id = :vehiculoId").bind("peso", p.pesoKg()).bind("vehiculoId", p.vehiculoId()).fetch().rowsUpdated()).then(cambiarEstado(id, "EN_RUTA")).then(cargar(id))
                    .doOnSuccess(resultado -> bus.publicar(new DespachoEvent(resultado.id(), resultado.estado(), "Despacho confirmado", resultado.trazaId(), Instant.now())));
        });
    }

    private Mono<Long> cambiarEstado(Long id, String estado) {
        return db.sql("UPDATE despacho SET estado = :estado WHERE id = :id").bind("estado", estado).bind("id", id).fetch().rowsUpdated();
    }
}
