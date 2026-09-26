package com.example.despachoreactive;

import com.example.despachoreactive.service.ServiciosExternosClient;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ServiciosExternosClientTest {
    @Test
    void tarifa_reintentaErroresTransitoriosYDevuelveRespuesta() {
        AtomicInteger llamadas = new AtomicInteger();
        ExchangeFunction exchange = request -> {
            if (llamadas.incrementAndGet() < 3) {
                return Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE).build());
            }
            return Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .body("123.45")
                    .build());
        };
        ServiciosExternosClient client = crearClient(exchange);

        StepVerifier.withVirtualTime(() -> client.tarifa("BOG", 10, BigDecimal.TEN))
                .thenAwait(Duration.ofSeconds(3))
                .expectNext(new BigDecimal("123.45"))
                .verifyComplete();

        assertThat(llamadas).hasValue(3);
    }

    @Test
    void tarifa_noReintentaErroresCuatroxxYUsaFallback() {
        AtomicInteger llamadas = new AtomicInteger();
        ExchangeFunction exchange = request -> {
            llamadas.incrementAndGet();
            return Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.BAD_REQUEST).build());
        };
        ServiciosExternosClient client = crearClient(exchange);

        StepVerifier.create(client.tarifa("BOG", 10, BigDecimal.TEN))
                .expectNext(BigDecimal.TEN)
                .verifyComplete();

        assertThat(llamadas).hasValue(1);
    }

    @Test
    void riesgo_timeoutDevuelveScorePorDefecto() {
        ExchangeFunction exchange = request -> Mono.never();
        ServiciosExternosClient client = crearClient(exchange);

        StepVerifier.withVirtualTime(() -> client.riesgo("BOG"))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(30)
                .verifyComplete();
    }

    @Test
    void ventana_cacheaPorCiudadDuranteTtl() {
        AtomicInteger llamadas = new AtomicInteger();
        ExchangeFunction exchange = request -> {
            llamadas.incrementAndGet();
            return Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN_VALUE)
                    .body("VENTANA_ESTANDAR")
                    .build());
        };
        ServiciosExternosClient client = crearClient(exchange);

        StepVerifier.create(Mono.zip(client.ventana("BOG"), client.ventana("BOG")))
                .expectNextMatches(tuple ->
                        "VENTANA_ESTANDAR".equals(tuple.getT1()) && "VENTANA_ESTANDAR".equals(tuple.getT2()))
                .verifyComplete();

        assertThat(llamadas).hasValue(1);
    }

    private ServiciosExternosClient crearClient(ExchangeFunction exchange) {
        return new ServiciosExternosClient(
                WebClient.builder().exchangeFunction(exchange).build(),
                Duration.ofSeconds(2),
                Duration.ofMillis(800),
                30
        );
    }
}