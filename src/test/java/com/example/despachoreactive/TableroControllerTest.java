package com.example.despachoreactive;

import com.example.despachoreactive.controller.TableroController;
import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;

class TableroControllerTest {
    @Test
    void tableroComparteEventosDelBus() {
        EventBus bus = new EventBus();
        TableroController controller = new TableroController(bus);
        DespachoEvent evento = new DespachoEvent(4L, "ASIGNADO", "ok", "trace", Instant.now());

        StepVerifier.create(controller.tablero().take(1))
                .then(() -> bus.publicar(evento))
                .expectNext(evento)
                .verifyComplete();
    }
}
