package com.example.despachoreactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.util.List;
import java.util.Map;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=18081",
                "app.external.base-url=http://localhost:18081"
        }
)
@AutoConfigureWebTestClient
class CupoConcurrenciaTest {
    @Autowired
    private DatabaseClient db;

    @LocalServerPort
    private int port;

    private WebClient http;

    @BeforeEach
    void setUp() {
        this.http = WebClient.builder().baseUrl("http://localhost:" + port).build();

        StepVerifier.create(resetDb()
                .then(seedVehiculo(1L, "ABC123", "BOG", 100)))
                .verifyComplete();

        http.delete()
                .uri("/external/simulator")
                .retrieve()
                .toBodilessEntity()
                .block();
    }

    @Test
    void concurrenciaNoDejaCupoNegativo() {
        Map<String, Object> request = Map.of(
                "clienteId", 200L,
                "ciudad", "BOG",
                "paquetes", List.of(Map.of("vehiculoId", 1L, "pesoKg", 10))
        );

        Mono<List<Integer>> statusCodes = Flux.range(1, 20)
                .flatMap(i -> http.post()
                        .uri("/api/despachos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(request)
                        .exchangeToMono(response -> Mono.just(response.statusCode().value())))
                .collectList();

        Mono<Integer> minCupo = db.sql("SELECT MIN(cupo_kg) min_cupo FROM vehiculo")
                .map((row, metadata) -> row.get("min_cupo", Integer.class))
                .one();

        StepVerifier.create(Mono.zip(statusCodes, minCupo))
                .expectNextMatches(this::validaResultadoConcurrencia)
                .verifyComplete();
    }

    private boolean validaResultadoConcurrencia(Tuple2<List<Integer>, Integer> result) {
        List<Integer> statuses = result.getT1();
        Integer minCupo = result.getT2();
        boolean estadosValidos = statuses.stream().allMatch(code -> code == 201 || code == 409 || code == 422);
        return estadosValidos && minCupo != null && minCupo >= 0;
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