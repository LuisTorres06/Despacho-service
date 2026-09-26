package com.example.despachoreactive;

import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SseEndpointsTest {
    @Autowired
    private EventBus bus;

    @LocalServerPort
    private int port;

    private WebClient http;

    @BeforeEach
    void setUp() {
        this.http = WebClient.builder().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void tableroHot_dosSuscriptoresRecibenMismoEvento() {
        DespachoEvent evento = new DespachoEvent(900L, "ASIGNADO", "evento tablero", "trace-sse", Instant.now());

        Flux<DespachoEvent> s1 = http.get()
                .uri("/api/ops/tablero")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(DespachoEvent.class)
                .take(1);

        Flux<DespachoEvent> s2 = http.get()
                .uri("/api/ops/tablero")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(DespachoEvent.class)
                .take(1);

        StepVerifier.create(Flux.zip(s1, s2))
                .thenAwait(Duration.ofMillis(150))
                .then(() -> bus.publicar(evento))
                .assertNext(tuple -> {
                    DespachoEvent primero = tuple.getT1();
                    DespachoEvent segundo = tuple.getT2();
                    org.assertj.core.api.Assertions.assertThat(primero).isEqualTo(evento);
                    org.assertj.core.api.Assertions.assertThat(segundo).isEqualTo(evento);
                })
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void eventsDeDespacho_cierraEnEstadoTerminal() {
        Long despachoId = 901L;

        Flux<DespachoEvent> stream = http.get()
                .uri("/api/despachos/{id}/events", despachoId)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(DespachoEvent.class);

        StepVerifier.create(stream)
                .thenAwait(Duration.ofMillis(150))
                .then(() -> bus.publicar(new DespachoEvent(despachoId, "ASIGNADO", "en proceso", "trace-events", Instant.now())))
                .expectNextMatches(evento -> "ASIGNADO".equals(evento.estado()))
                .then(() -> bus.publicar(new DespachoEvent(despachoId, "EXPIRADO", "terminal", "trace-events", Instant.now())))
                .expectNextMatches(evento -> "EXPIRADO".equals(evento.estado()))
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }
}