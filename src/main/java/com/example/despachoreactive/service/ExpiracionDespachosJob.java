package com.example.despachoreactive.service;

import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.Clock;
import java.time.Duration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component
public class ExpiracionDespachosJob {
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
        suscripcion = Flux.interval(intervalo, scheduler).onBackpressureDrop()
                .concatMap(tick -> expirar().onErrorResume(error -> Mono.empty())).subscribe();
    }

    Mono<Long> expirar() {
        return db.sql("SELECT id FROM despacho WHERE estado = 'ASIGNADO' AND expira_en < :ahora").bind("ahora", Instant.now(reloj)).map((r, m) -> r.get("id", Long.class)).all()
            .concatMap(id -> db.sql("SELECT vehiculo_id, peso_kg FROM paquete WHERE despacho_id = :id").bind("id", id).map((r, m) -> new long[]{r.get("vehiculo_id", Long.class), r.get("peso_kg", Integer.class)}).all()
                .concatMap(p -> db.sql("UPDATE vehiculo SET cupo_kg = cupo_kg + :peso, reservado_kg = GREATEST(reservado_kg - :peso, 0) WHERE id = :vehiculoId").bind("peso", p[1]).bind("vehiculoId", p[0]).fetch().rowsUpdated())
                .then(db.sql("UPDATE despacho SET estado = 'EXPIRADO', expira_en = NULL WHERE id = :id").bind("id", id).fetch().rowsUpdated())
                .doOnSuccess(n -> bus.publicar(new com.example.despachoreactive.dto.DespachoEvent(id, "EXPIRADO", "Despacho expirado", "job", Instant.now(reloj))))).count();
    }

    @PreDestroy
    public void detener() {
        if (suscripcion != null) suscripcion.dispose();
    }
}
