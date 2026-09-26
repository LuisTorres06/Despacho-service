package com.example.despachoreactive.controller;

import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.dto.DespachoRequest;
import com.example.despachoreactive.dto.DespachoResponse;
import com.example.despachoreactive.service.DespachoService;
import com.example.despachoreactive.service.EventBus;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Set;

@RestController
@RequestMapping("/api/despachos")
public class DespachoController {
    private static final Set<String> TERMINALES = Set.of("ENTREGADO", "RECHAZADO", "EXPIRADO");
    private final DespachoService service;
    private final EventBus bus;

    public DespachoController(DespachoService service, EventBus bus) {
        this.service = service;
        this.bus = bus;
    }

    @PostMapping
    public Mono<ResponseEntity<DespachoResponse>> crear(
            @Valid @RequestBody DespachoRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idemKey
    ) {
        return service.crear(request, idemKey)
                .map(result -> ResponseEntity.status(201).body(result));
    }

    @GetMapping("/{id}")
    public Mono<DespachoResponse> buscar(@PathVariable Long id) {
        return service.buscar(id);
    }

    @PostMapping("/{id}/confirm")
    public Mono<DespachoResponse> confirmar(@PathVariable Long id) {
        return service.confirmar(id);
    }

    @GetMapping(value = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<DespachoEvent> eventos(@PathVariable Long id) {
        return bus.eventos()
                .filter(evento -> evento.despachoId().equals(id))
                .takeUntil(evento -> TERMINALES.contains(evento.estado()))
                .timeout(Duration.ofMinutes(15));
    }
}