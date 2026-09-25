package com.example.despachoreactive.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.WebFilter;

import java.util.UUID;

@Configuration
public class TraceWebFilter {
    public static final String KEY = "trazaId";

    @Bean
    WebFilter traceFilter() {
        return (exchange, chain) -> {
            String trace = exchange.getRequest().getHeaders().getFirst("X-Traza-Id");
            String value = trace == null || trace.isBlank() ? UUID.randomUUID().toString() : trace;
            exchange.getResponse().getHeaders().add("X-Traza-Id", value);
            return chain.filter(exchange).contextWrite(context -> context.put(KEY, value));
        };
    }
}
