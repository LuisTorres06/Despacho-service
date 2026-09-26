package com.example.despachoreactive.controller;

import com.example.despachoreactive.dto.DespachoEvent;
import com.example.despachoreactive.service.EventBus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/ops")
public class TableroController {
    private final EventBus bus;

    public TableroController(EventBus bus) {
        this.bus = bus;
    }

    @GetMapping(value = "/tablero", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<DespachoEvent> tablero() {
        return bus.eventos()
                //comparte stream hot, se conecta con primer suscriptor
                .publish().refCount(1)
                //backpressure para consumidor lento -> conserva lo más reciente
                .onBackpressureLatest();
    }
}