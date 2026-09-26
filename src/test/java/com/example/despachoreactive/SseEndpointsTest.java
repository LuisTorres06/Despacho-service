package com.example.despachoreactive;

import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class SseEndpointsTest {
    @Autowired
    private WebTestClient client;

    @Autowired
    private EventBus bus;

    @Test
    void tableroHot_dosSuscriptoresRecibenMismoEvento() {
        DespachoEvent evento = new DespachoEvent(900L, "ASIGNADO", "evento tablero", "trace-sse", Instant.now());

        Flux<DespachoEvent> s1 = client.get()
                .uri("/api/ops/tablero")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .returnResult(DespachoEvent.class)
                .getResponseBody()
                .take(1);

        Flux<DespachoEvent> s2 = client.get()
                .uri("/api/ops/tablero")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .returnResult(DespachoEvent.class)
                .getResponseBody()
                .take(1);

        StepVerifier.create(Flux.zip(s1, s2))
                .then(() -> bus.publicar(evento))
                .assertNext(tuple -> {
                    DespachoEvent primero = tuple.getT1();
                    DespachoEvent segundo = tuple.getT2();
                    org.assertj.core.api.Assertions.assertThat(primero).isEqualTo(evento);
                    org.assertj.core.api.Assertions.assertThat(segundo).isEqualTo(evento);
                })
                .verifyComplete();
    }

    @Test
    void eventsDeDespacho_cierraEnEstadoTerminal() {
        Long despachoId = 901L;

        Flux<DespachoEvent> stream = client.get()
                .uri("/api/despachos/{id}/events", despachoId)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .returnResult(DespachoEvent.class)
                .getResponseBody();

        StepVerifier.create(stream)
                .then(() -> bus.publicar(new DespachoEvent(despachoId, "ASIGNADO", "en proceso", "trace-events", Instant.now())))
                .expectNextMatches(evento -> "ASIGNADO".equals(evento.estado()))
                .then(() -> bus.publicar(new DespachoEvent(despachoId, "EXPIRADO", "terminal", "trace-events", Instant.now())))
                .expectNextMatches(evento -> "EXPIRADO".equals(evento.estado()))
                .verifyComplete();
    }
}