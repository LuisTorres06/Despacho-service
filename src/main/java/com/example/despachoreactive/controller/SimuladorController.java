package com.example.despachoreactive.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/external")
public class SimuladorController {
    private final AtomicInteger fallasTarifaRestantes = new AtomicInteger(0);
    private final AtomicInteger latenciaRiesgoMs = new AtomicInteger(0);
    private final AtomicInteger scoreRiesgo = new AtomicInteger(30);

    @GetMapping("/simulator")
    public Map<String, Object> estado() {
        return Map.of(
                "fallasTarifa", fallasTarifaRestantes.get(),
                "latenciaRiesgoMs", latenciaRiesgoMs.get(),
                "scoreRiesgo", scoreRiesgo.get()
        );
    }

    @PutMapping("/simulator")
    public Mono<Map<String, Object>> configurar(@RequestBody Map<String, Integer> body) {
        if (body.containsKey("fallasTarifa")) {
            fallasTarifaRestantes.set(Math.max(0, body.get("fallasTarifa")));
        }
        if (body.containsKey("latenciaRiesgoMs")) {
            latenciaRiesgoMs.set(Math.max(0, body.get("latenciaRiesgoMs")));
        }
        if (body.containsKey("scoreRiesgo")) {
            scoreRiesgo.set(body.get("scoreRiesgo"));
        }

        if (body.containsKey("fallo")) {
            fallasTarifaRestantes.set(body.get("fallo") != 0 ? 1 : 0);
        }
        if (body.containsKey("latenciaMs")) {
            latenciaRiesgoMs.set(Math.max(0, body.get("latenciaMs")));
        }
        if (body.containsKey("riesgo")) {
            scoreRiesgo.set(body.get("riesgo"));
        }

        return Mono.just(estado());
    }

    @DeleteMapping("/simulator")
    public Mono<Map<String, Object>> reset() {
        fallasTarifaRestantes.set(0);
        latenciaRiesgoMs.set(0);
        scoreRiesgo.set(30);
        return Mono.just(estado());
    }

    @GetMapping("/pricing")
    public Mono<BigDecimal> pricing(@RequestParam String ciudad, @RequestParam int peso) {
        int restantesAntes = fallasTarifaRestantes.getAndUpdate(v -> Math.max(v - 1, 0));
        if (restantesAntes > 0) {
            return Mono.error(new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Fallo simulado de tarifa"));
        }
        return Mono.just(BigDecimal.valueOf(10000L + (long) peso * 10L));
    }

    @GetMapping("/risk")
    public Mono<Integer> risk(@RequestParam String ciudad) {
        return Mono.delay(Duration.ofMillis(latenciaRiesgoMs.get()))
                .thenReturn(scoreRiesgo.get());
    }

    @GetMapping("/window")
    public Mono<String> window(@RequestParam String ciudad) {
        return Mono.just("VENTANA_ESTANDAR");
    }
}