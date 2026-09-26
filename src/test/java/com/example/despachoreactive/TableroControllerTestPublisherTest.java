package com.example.despachoreactive;

import com.example.despachoreactive.controller.TableroController;
import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Instant;

class TableroControllerTestPublisherTest {
    @Test
    void tablero_emiteEventosDesdeFuenteControladaConTestPublisher() {
        EventBus bus = Mockito.mock(EventBus.class);
        TestPublisher<DespachoEvent> publisher = TestPublisher.create();
        Mockito.when(bus.eventos()).thenReturn(publisher.flux());
        TableroController controller = new TableroController(bus);

        DespachoEvent e1 = new DespachoEvent(10L, "ASIGNADO", "uno", "t1", Instant.now());
        DespachoEvent e2 = new DespachoEvent(10L, "EN_RUTA", "dos", "t1", Instant.now());

        StepVerifier.create(controller.tablero().take(2))
                .then(() -> publisher.next(e1, e2))
                .expectNext(e1, e2)
                .verifyComplete();
    }

    @Test
    void tablero_propagErrorDeFuenteConTestPublisher() {
        EventBus bus = Mockito.mock(EventBus.class);
        TestPublisher<DespachoEvent> publisher = TestPublisher.create();
        Mockito.when(bus.eventos()).thenReturn(publisher.flux());
        TableroController controller = new TableroController(bus);

        StepVerifier.create(controller.tablero())
                .then(() -> publisher.error(new RuntimeException("fallo-controlado")))
                .expectErrorMatches(error -> "fallo-controlado".equals(error.getMessage()))
                .verify();
    }
}