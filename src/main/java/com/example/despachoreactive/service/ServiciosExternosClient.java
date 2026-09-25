package com.example.despachoreactive.service;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;

@Service
public class ServiciosExternosClient {
    private final WebClient client;

    public ServiciosExternosClient(WebClient externalWebClient) {
        this.client = externalWebClient;
    }

    public Mono<BigDecimal> tarifa(String ciudad, int peso, BigDecimal fallback) {
        return client.get().uri(uri -> uri.path("/external/pricing").queryParam("ciudad", ciudad).queryParam("peso", peso).build()).retrieve().bodyToMono(BigDecimal.class)
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200)).filter(this::transitorio))
                .onErrorReturn(fallback);
    }

    public Mono<Integer> riesgo(String ciudad) {
        return client.get().uri(uri -> uri.path("/external/risk").queryParam("ciudad", ciudad).build()).retrieve().bodyToMono(Integer.class)
                .timeout(Duration.ofMillis(800)).onErrorReturn(30);
    }

    public Mono<String> ventana(String ciudad) {
        return client.get().uri(uri -> uri.path("/external/window").queryParam("ciudad", ciudad).build()).retrieve().bodyToMono(String.class).cache(Duration.ofMinutes(10));
    }

    private boolean transitorio(Throwable error) {
        return !(error instanceof org.springframework.web.reactive.function.client.WebClientResponseException response) || response.getStatusCode().is5xxServerError();
    }
}
