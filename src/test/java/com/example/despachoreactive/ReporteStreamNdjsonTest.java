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

import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class ReporteStreamNdjsonTest {
    @Autowired
    private WebTestClient client;

    @Autowired
    private DatabaseClient db;

    @BeforeEach
    void setUp() {
        StepVerifier.create(resetDb().then(seedData()))
                .verifyComplete();
    }

    @Test
    void reporteStream_ndjsonAcumulaPorCiudad() {
        StepVerifier.create(client.get()
                        .uri("/api/reports/ciudades/stream")
                        .accept(MediaType.APPLICATION_NDJSON)
                        .exchange()
                        .returnResult(Map.class)
                        .getResponseBody()
                        .take(2))
                .expectNextMatches(mapa -> !mapa.containsKey("ciudades"))
                .expectNextMatches(mapa -> {
                    Object ciudadesObj = mapa.get("ciudades");
                    if (!(ciudadesObj instanceof Map<?, ?> ciudades)) {
                        return false;
                    }
                    return ciudades.containsKey("BOG");
                })
                .verifyComplete();
    }

    private Mono<Void> resetDb() {
        return db.sql("DELETE FROM paquete").fetch().rowsUpdated()
                .then(db.sql("DELETE FROM despacho").fetch().rowsUpdated())
                .then(db.sql("DELETE FROM vehiculo").fetch().rowsUpdated())
                .then();
    }

    private Mono<Void> seedData() {
        return db.sql("INSERT INTO vehiculo (id, placa, ciudad, cupo_kg, reservado_kg) VALUES (1, 'ABC123', 'BOG', 500, 0)")
                .fetch().rowsUpdated()
                .then(db.sql("INSERT INTO despacho (cliente_id, ciudad, estado, tarifa, total, score_riesgo, traza_id, idem_key, expira_en) VALUES (1, 'BOG', 'ASIGNADO', 10000, 10000, 30, 'trace-r', 'idem-r', now() + interval '15 minutes') RETURNING id")
                        .map((row, metadata) -> row.get("id", Long.class))
                        .one()
                        .flatMap(despachoId -> db.sql("INSERT INTO paquete (despacho_id, vehiculo_id, peso_kg) VALUES (:despachoId, 1, 50)")
                                .bind("despachoId", despachoId)
                                .fetch()
                                .rowsUpdated()
                                .then()))
                .then();
    }
}