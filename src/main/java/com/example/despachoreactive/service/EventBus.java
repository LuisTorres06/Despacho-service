package com.example.despachoreactive.service;

import com.example.despachoreactive.dto.DespachoEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Component
public class EventBus {
    private final Sinks.Many<DespachoEvent> eventos = Sinks.many().multicast().directBestEffort();

    public void publicar(DespachoEvent evento) {
        eventos.tryEmitNext(evento);
    }

    public Flux<DespachoEvent> eventos() {
        return eventos.asFlux();
    }
}