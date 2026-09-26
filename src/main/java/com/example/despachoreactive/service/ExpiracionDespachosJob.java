package com.example.despachoreactive.service;

import com.example.despachoreactive.dto.DespachoEvent;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Component
public class ExpiracionDespachosJob {
    private static final Logger log = LoggerFactory.getLogger(ExpiracionDespachosJob.class);

    private final DatabaseClient db;
    private final EventBus bus;
    private final Duration intervalo;
    private final Clock reloj;
    private final Scheduler scheduler;
    private Disposable suscripcion;

    public ExpiracionDespachosJob(DatabaseClient db, EventBus bus) {
        this(db, bus, Duration.ofSeconds(30), Clock.systemUTC(), Schedulers.parallel());
    }

    public ExpiracionDespachosJob(DatabaseClient db, EventBus bus, Duration intervalo, Clock reloj, Scheduler scheduler) {
        this.db = db;
        this.bus = bus;
        this.intervalo = intervalo;
        this.reloj = reloj;
        this.scheduler = scheduler;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void iniciar() {
        suscripcion = Flux.interval(intervalo, scheduler)
                .onBackpressureDrop()
                .concatMap(tick -> expirar()
                        .doOnError(error -> log.error("Error ejecutando expiracion de despachos", error))
                        .onErrorResume(error -> Mono.empty()))
                .subscribe();
    }

    Mono<Long> expirar() {
        return db.sql("SELECT id FROM despacho WHERE estado = 'ASIGNADO' AND expira_en < :ahora")
                .bind("ahora", Instant.now(reloj))
                .map((row, metadata) -> row.get("id", Long.class))
                .all()
                .concatMap(despachoId -> db.sql("SELECT vehiculo_id, peso_kg FROM paquete WHERE despacho_id = :id")
                        .bind("id", despachoId)
                        .map((row, metadata) -> new long[]{
                                row.get("vehiculo_id", Long.class),
                                row.get("peso_kg", Integer.class)
                        })
                        .all()
                        .concatMap(paquete -> db.sql("""
                                        UPDATE vehiculo
                                        SET cupo_kg = cupo_kg + :peso,
                                            reservado_kg = GREATEST(reservado_kg - :peso, 0)
                                        WHERE id = :vehiculoId
                                        """)
                                .bind("peso", paquete[1])
                                .bind("vehiculoId", paquete[0])
                                .fetch()
                                .rowsUpdated())
                        .then(db.sql("UPDATE despacho SET estado = 'EXPIRADO', expira_en = NULL WHERE id = :id")
                                .bind("id", despachoId)
                                .fetch()
                                .rowsUpdated())
                        .doOnSuccess(rows -> bus.publicar(new DespachoEvent(
                                despachoId,
                                "EXPIRADO",
                                "Despacho expirado",
                                "job",
                                Instant.now(reloj)
                        ))))
                .count();
    }

    @PreDestroy
    public void detener() {
        if (suscripcion != null) {
            suscripcion.dispose();
        }
    }
}