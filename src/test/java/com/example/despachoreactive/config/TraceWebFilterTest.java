package com.example.despachoreactive.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.mock.web.server.MockServerWebExchange.Builder;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class TraceWebFilterTest {
    @Test
    void conservaTrazaRecibidaYLaPropagaEnContexto() {
        TraceWebFilter configuration = new TraceWebFilter();
        MockServerWebExchange exchange = MockServerWebExchange.from(org.springframework.mock.http.server.reactive.MockServerHttpRequest.get("/").header("X-Traza-Id", "trace-known").build());

        StepVerifier.create(configuration.traceFilter().filter(exchange, chainedExchange -> Mono.deferContextual(context -> {
            Object trace = context.get(TraceWebFilter.KEY);
            assertThat(trace).isEqualTo("trace-known");
            return chainedExchange.getResponse().setComplete();
        }))).verifyComplete();

        assertThat(exchange.getResponse().getHeaders().getFirst("X-Traza-Id")).isEqualTo("trace-known");
    }

    @Test
    void generaTrazaCuandoNoSeRecibeHeader() {
        TraceWebFilter configuration = new TraceWebFilter();
        MockServerWebExchange exchange = MockServerWebExchange.from(org.springframework.mock.http.server.reactive.MockServerHttpRequest.get("/").build());

        StepVerifier.create(configuration.traceFilter().filter(exchange, chainedExchange -> Mono.deferContextual(context -> {
            Object trace = context.get(TraceWebFilter.KEY);
            assertThat(trace).isNotNull();
            return chainedExchange.getResponse().setComplete();
        }))).verifyComplete();

        assertThat(exchange.getResponse().getHeaders().getFirst("X-Traza-Id")).isNotBlank();
    }
}
