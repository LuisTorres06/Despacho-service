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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class VehiculoBulkNdjsonTest {
    @Autowired
    private WebTestClient client;

    @Autowired
    private DatabaseClient db;

    @BeforeEach
    void setUp() {
        StepVerifier.create(db.sql("DELETE FROM vehiculo").fetch().rowsUpdated().then())
                .verifyComplete();
    }

    @Test
    void bulkNdjson_procesaLoteYHaceUpsert() {
        String ndjson = """
                {"id":1,"placa":"AAA111","ciudad":"BOG","cupoKg":500}
                {"id":2,"placa":"BBB222","ciudad":"MDE","cupoKg":300}
                """;

        client.post()
                .uri("/api/vehiculos/bulk")
                .contentType(MediaType.APPLICATION_NDJSON)
                .bodyValue(ndjson)
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.procesados").isEqualTo(2);

        Mono<Long> total = db.sql("SELECT COUNT(*) total FROM vehiculo WHERE id IN (1,2)")
                .map((row, metadata) -> row.get("total", Long.class))
                .one();

        StepVerifier.create(total)
                .expectNext(2L)
                .verifyComplete();
    }
}