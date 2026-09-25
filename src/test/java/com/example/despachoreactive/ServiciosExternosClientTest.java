package com.example.despachoreactive;

import com.example.despachoreactive.service.ServiciosExternosClient;
import org.junit.jupiter.api.Test;
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
            return Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK).header(org.springframework.http.HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE).body("123.45").build());
        };
        ServiciosExternosClient client = new ServiciosExternosClient(WebClient.builder().exchangeFunction(exchange).build());

        StepVerifier.withVirtualTime(() -> client.tarifa("BOG", 10, BigDecimal.TEN))
                .thenAwait(Duration.ofSeconds(2))
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
        ServiciosExternosClient client = new ServiciosExternosClient(WebClient.builder().exchangeFunction(exchange).build());

        StepVerifier.create(client.tarifa("BOG", 10, BigDecimal.TEN))
                .expectNext(BigDecimal.TEN)
                .verifyComplete();

        assertThat(llamadas).hasValue(1);
    }

    @Test
    void riesgo_timeoutDevuelveScorePorDefecto() {
        ExchangeFunction exchange = request -> Mono.never();
        ServiciosExternosClient client = new ServiciosExternosClient(WebClient.builder().exchangeFunction(exchange).build());

        StepVerifier.withVirtualTime(() -> client.riesgo("BOG"))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(30)
                .verifyComplete();
    }
}