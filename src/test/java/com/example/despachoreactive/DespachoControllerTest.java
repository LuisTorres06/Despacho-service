package com.example.despachoreactive;

import com.example.despachoreactive.controller.DespachoController;
import com.example.despachoreactive.dto.DespachoRequest;
import com.example.despachoreactive.dto.DespachoResponse;
import com.example.despachoreactive.service.DespachoService;
import com.example.despachoreactive.service.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DespachoControllerTest {
    private DespachoService service;
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        service = mock(DespachoService.class);
        client = WebTestClient.bindToController(new DespachoController(service, mock(EventBus.class))).build();
    }

    @Test
    void crear_devuelve201YPropagaIdempotencyKey() {
        DespachoResponse response = new DespachoResponse(
                7L, 9L, "BOG", "ASIGNADO",
                BigDecimal.TEN, BigDecimal.TEN, 20,
                "trace-1", Instant.now(), Instant.now(), List.of()
        );
        when(service.crear(any(), eq("key-1"))).thenReturn(Mono.just(response));

        client.post().uri("/api/despachos")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", "key-1")
                .bodyValue(new DespachoRequest(9L, "BOG", List.of(new DespachoRequest.PaqueteRequest(1L, 20))))
                .exchange()
                .expectStatus().isCreated()
                .expectBody().jsonPath("$.id").isEqualTo(7);

        verify(service).crear(any(), eq("key-1"));
    }

    @Test
    void crear_rechazaPaquetesVacios() {
        client.post().uri("/api/despachos")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new DespachoRequest(9L, "BOG", List.of()))
                .exchange()
                .expectStatus().isBadRequest();
    }
}