package com.example.despachoreactive.controller;

import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/external/simulator")
public class SimuladorController {
    private final AtomicBoolean fallo = new AtomicBoolean();
    private final AtomicInteger latencia = new AtomicInteger();
    private final AtomicInteger riesgo = new AtomicInteger(30);

    @GetMapping
    public Map<String, Object> estado() {
        return Map.of("fallo", fallo.get(), "latenciaMs", latencia.get(), "riesgo", riesgo.get());
    }

    @PutMapping
    public Mono<Map<String, Object>> configurar(@RequestBody Map<String, Integer> body) {
        if (body.containsKey("fallo")) fallo.set(body.get("fallo") != 0);
        if (body.containsKey("latenciaMs")) latencia.set(Math.max(0, body.get("latenciaMs")));
        if (body.containsKey("riesgo")) riesgo.set(body.get("riesgo"));
        return Mono.just(estado());
    }

    @DeleteMapping
    public Mono<Void> reset() {
        fallo.set(false);
        latencia.set(0);
        riesgo.set(30);
        return Mono.empty();
    }

    @GetMapping("/pricing")
    public Mono<BigDecimal> pricing(@RequestParam String ciudad, @RequestParam int peso) {
        return respuesta(() -> BigDecimal.valueOf(10000L + peso * 10L));
    }

    @GetMapping("/risk")
    public Mono<Integer> risk(@RequestParam String ciudad) {
        return respuesta(riesgo::get);
    }

    @GetMapping("/window")
    public Mono<String> window(@RequestParam String ciudad) {
        return respuesta(() -> "VENTANA_ESTANDAR");
    }

    private <T> Mono<T> respuesta(java.util.function.Supplier<T> supplier) {
        if (fallo.get()) return Mono.error(new IllegalStateException("Fallo simulado"));
        return Mono.delay(java.time.Duration.ofMillis(latencia.get())).thenReturn(supplier.get());
    }
}
