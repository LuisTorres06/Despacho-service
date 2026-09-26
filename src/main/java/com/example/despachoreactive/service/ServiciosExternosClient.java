package com.example.despachoreactive.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class ServiciosExternosClient {
    private final WebClient client;
    private final Duration pricingTimeout;
    private final Duration riskTimeout;
    private final int defaultRiskScore;
    private final ConcurrentMap<String, Mono<String>> ventanaCache;

    public ServiciosExternosClient(
            WebClient externalWebClient,
            @Value("${app.external.pricing-timeout:2s}") Duration pricingTimeout,
            @Value("${app.external.risk-timeout:800ms}") Duration riskTimeout,
            @Value("${app.default-risk-score:30}") int defaultRiskScore
    ) {
        this.client = externalWebClient;
        this.pricingTimeout = pricingTimeout;
        this.riskTimeout = riskTimeout;
        this.defaultRiskScore = defaultRiskScore;
        this.ventanaCache = new ConcurrentHashMap<>();
    }

    public Mono<BigDecimal> tarifa(String ciudad, int peso, BigDecimal fallback) {
        return client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/external/pricing")
                        .queryParam("ciudad", ciudad)
                        .queryParam("peso", peso)
                        .build())
                .retrieve()
                .bodyToMono(BigDecimal.class)
                // timeout para no colgar flujo por dependencia
                .timeout(pricingTimeout)
                // retry con backoff SOLO cuando error es transitorio
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200)).filter(this::transitorio))
                // fallback controlado si todo falla
                .onErrorReturn(fallback);
    }

    public Mono<Integer> riesgo(String ciudad) {
        return client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/external/risk")
                        .queryParam("ciudad", ciudad)
                        .build())
                .retrieve()
                .bodyToMono(Integer.class)
                .timeout(riskTimeout)
                // Fallback de score por defecto
                .onErrorReturn(defaultRiskScore);
    }

    public Mono<String> ventana(String ciudad) {
        // Cache por ciudad
        return ventanaCache.computeIfAbsent(ciudad, key -> client.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/external/window")
                        .queryParam("ciudad", key)
                        .build())
                .retrieve()
                .bodyToMono(String.class)
                // cache TTL 10 min (cold->hot cacheado por clave)
                .cache(Duration.ofMinutes(10))
                // Si falla, limpia cache para no dejar fallo pegado
                .doOnError(error -> ventanaCache.remove(key)));
    }

    private boolean transitorio(Throwable error) {
        // Reintenta errores de red/5xx; NO 4xx de cliente
        return !(error instanceof WebClientResponseException responseException)
                || responseException.getStatusCode().is5xxServerError();
    }
}