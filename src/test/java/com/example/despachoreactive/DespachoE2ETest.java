package com.example.despachoreactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class DespachoE2ETest {
    @Autowired
    private WebTestClient client;

    @Autowired
    private DatabaseClient db;

    @BeforeEach
    void setUp() {
        StepVerifier.create(resetDb()
                .then(seedVehiculo(1L, "ABC123", "BOG", 500)))
                .verifyComplete();

        client.delete()
                .uri("/external/simulator")
                .exchange()
                .expectStatus()
                .isOk();
    }

    @Test
    void crearYConfirmarDespacho_flujoCompleto() {
        Map<String, Object> request = Map.of(
                "clienteId", 101L,
                "ciudad", "BOG",
                "paquetes", List.of(Map.of("vehiculoId", 1L, "pesoKg", 120))
        );

        AtomicLong despachoId = new AtomicLong();

        client.post()
                .uri("/api/despachos")
                .header("X-Traza-Id", "trace-e2e-1")
                .header("Idempotency-Key", "IDEMP-E2E-1")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus()
                .isCreated()
                .expectBody()
                .jsonPath("$.estado").isEqualTo("ASIGNADO")
                .jsonPath("$.ciudad").isEqualTo("BOG")
                .jsonPath("$.id").value(value -> despachoId.set(Long.parseLong(value.toString())));

        client.get()
                .uri("/api/despachos/{id}", despachoId.get())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.id").isEqualTo((int) despachoId.get())
                .jsonPath("$.estado").isEqualTo("ASIGNADO")
                .jsonPath("$.paquetes[0].vehiculoId").isEqualTo(1);

        client.post()
                .uri("/api/despachos/{id}/confirm", despachoId.get())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.estado").isEqualTo("EN_RUTA");
    }

    private Mono<Void> resetDb() {
        return db.sql("DELETE FROM paquete").fetch().rowsUpdated()
                .then(db.sql("DELETE FROM despacho").fetch().rowsUpdated())
                .then(db.sql("DELETE FROM vehiculo").fetch().rowsUpdated())
                .then();
    }

    private Mono<Void> seedVehiculo(Long id, String placa, String ciudad, int cupoKg) {
        return db.sql("INSERT INTO vehiculo (id, placa, ciudad, cupo_kg, reservado_kg) VALUES (:id, :placa, :ciudad, :cupo, 0)")
                .bind("id", id)
                .bind("placa", placa)
                .bind("ciudad", ciudad)
                .bind("cupo", cupoKg)
                .fetch()
                .rowsUpdated()
                .then();
    }
}