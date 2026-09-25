package com.example.despachoreactive;

import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class EventBusTest {
    @Test
    void publicaEventosEnElBusMulticast() {
        EventBus bus = new EventBus();
        DespachoEvent evento = new DespachoEvent(1L, "ASIGNADO", "ok", "traza", Instant.now());

        StepVerifier.create(bus.eventos().take(1).concatWith(reactor.core.publisher.Mono.never()))
                .then(() -> bus.publicar(evento))
                .expectNext(evento)
                .thenCancel()
                .verify();
    }

    @Test
    void dosSuscriptoresRecibenElMismoEvento() {
        EventBus bus = new EventBus();
        DespachoEvent evento = new DespachoEvent(2L, "ASIGNADO", "ok", "traza", Instant.now());
        List<DespachoEvent> primero = new CopyOnWriteArrayList<>();
        List<DespachoEvent> segundo = new CopyOnWriteArrayList<>();

        var suscripcion1 = bus.eventos().subscribe(primero::add);
        var suscripcion2 = bus.eventos().subscribe(segundo::add);
        bus.publicar(evento);

        org.assertj.core.api.Assertions.assertThat(primero).containsExactly(evento);
        org.assertj.core.api.Assertions.assertThat(segundo).containsExactly(evento);
        suscripcion1.dispose();
        suscripcion2.dispose();
    }

    @Test
    void suscriptorTardioNoRecibeEventosAnteriores() {
        EventBus bus = new EventBus();
        bus.publicar(new DespachoEvent(3L, "ASIGNADO", "anterior", "traza", Instant.now()));

        StepVerifier.create(bus.eventos())
                .thenCancel()
                .verify();
    }
}
