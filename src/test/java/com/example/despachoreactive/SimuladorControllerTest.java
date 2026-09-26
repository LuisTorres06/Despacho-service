package com.example.despachoreactive;

import com.example.despachoreactive.controller.SimuladorController;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SimuladorControllerTest {
    @Test
    void configuraSimuladorConPayloadDelTaller() {
        SimuladorController simulador = new SimuladorController();

        StepVerifier.create(simulador.configurar(Map.of(
                        "fallasTarifa", 2,
                        "latenciaRiesgoMs", 10,
                        "scoreRiesgo", 95
                )))
                .assertNext(estado -> assertThat(estado)
                        .containsEntry("fallasTarifa", 2)
                        .containsEntry("latenciaRiesgoMs", 10)
                        .containsEntry("scoreRiesgo", 95))
                .verifyComplete();
    }

    @Test
    void pricing_fallaLasVecesConfiguradasYLuegoResponde() {
        SimuladorController simulador = new SimuladorController();

        StepVerifier.create(simulador.configurar(Map.of("fallasTarifa", 2)).then())
                .verifyComplete();

        StepVerifier.create(simulador.pricing("BOG", 10))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(ResponseStatusException.class);
                    ResponseStatusException responseError = (ResponseStatusException) error;
                    assertThat(responseError.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                })
                .verify();

        StepVerifier.create(simulador.pricing("BOG", 10))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(ResponseStatusException.class);
                    ResponseStatusException responseError = (ResponseStatusException) error;
                    assertThat(responseError.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                })
                .verify();

        StepVerifier.create(simulador.pricing("BOG", 10))
                .expectNext(BigDecimal.valueOf(10100))
                .verifyComplete();
    }

    @Test
    void endpointsDevuelvenValoresPorDefecto() {
        SimuladorController simulador = new SimuladorController();

        StepVerifier.create(simulador.pricing("BOG", 10))
                .expectNext(BigDecimal.valueOf(10100))
                .verifyComplete();

        StepVerifier.create(simulador.risk("BOG"))
                .expectNext(30)
                .verifyComplete();

        StepVerifier.create(simulador.window("BOG"))
                .expectNext("VENTANA_ESTANDAR")
                .verifyComplete();
    }

    @Test
    void resetLimpiaElEstadoDelSimulador() {
        SimuladorController simulador = new SimuladorController();

        StepVerifier.create(simulador.configurar(Map.of(
                        "fallasTarifa", 1,
                        "latenciaRiesgoMs", 100,
                        "scoreRiesgo", 90
                )).then(simulador.reset()).then(simulador.risk("BOG")))
                .expectNext(30)
                .verifyComplete();

        assertThat(simulador.estado())
                .containsEntry("fallasTarifa", 0)
                .containsEntry("latenciaRiesgoMs", 0)
                .containsEntry("scoreRiesgo", 30);
    }

    @Test
    void respetaLatenciaRiesgoConTiempoVirtual() {
        SimuladorController simulador = new SimuladorController();

        StepVerifier.withVirtualTime(() -> simulador.configurar(Map.of("latenciaRiesgoMs", 500))
                        .then(simulador.risk("BOG")))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(500))
                .expectNext(30)
                .verifyComplete();
    }
}