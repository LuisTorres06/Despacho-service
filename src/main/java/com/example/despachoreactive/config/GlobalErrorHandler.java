package com.example.despachoreactive.config;

import com.example.despachoreactive.service.DomainException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class GlobalErrorHandler {
    @ExceptionHandler(DomainException.class)
    public Mono<ResponseEntity<Map<String, Object>>> domain(DomainException error) {
        return Mono.deferContextual(context -> Mono.just(ResponseEntity.status(error.status()).body(body(error.getMessage(), context.getOrDefault(TraceWebFilter.KEY, "n/a"), error.status()))));
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ResponseEntity<Map<String, Object>>> validation(WebExchangeBindException error) {
        return Mono.deferContextual(context -> Mono.just(ResponseEntity.badRequest().body(body("Solicitud invalida", context.getOrDefault(TraceWebFilter.KEY, "n/a"), HttpStatus.BAD_REQUEST))));
    }

    private Map<String, Object> body(String message, String trace, HttpStatus status) {
        return Map.of("codigo", status.value(), "mensaje", message, "trazaId", trace, "instante", Instant.now().toString());
    }
}
