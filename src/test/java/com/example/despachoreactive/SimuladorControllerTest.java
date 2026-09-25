package com.example.despachoreactive;

import com.example.despachoreactive.controller.SimuladorController;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SimuladorControllerTest {
    @Test
    void configuraFalloLatenciaYRiesgo() {
        SimuladorController simulador = new SimuladorController();

        StepVerifier.create(simulador.configurar(Map.of("fallo", 1, "latenciaMs", 10, "riesgo", 90)))
                .assertNext(estado -> assertThat(estado).containsEntry("fallo", true).containsEntry("latenciaMs", 10).containsEntry("riesgo", 90))
                .verifyComplete();

        StepVerifier.create(simulador.pricing("BOG", 10))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void endpointsDevuelvenValoresCuandoNoHayFallo() {
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
        simulador.configurar(Map.of("fallo", 1, "latenciaMs", 100, "riesgo", 90)).block();

        StepVerifier.create(simulador.reset().then(simulador.risk("BOG")))
                .expectNext(30)
                .verifyComplete();
        assertThat(simulador.estado()).containsEntry("fallo", false).containsEntry("latenciaMs", 0);
    }

        @Test
        void respetaLaLatenciaConfiguradaConTiempoVirtual() {
                SimuladorController simulador = new SimuladorController();
                simulador.configurar(Map.of("latenciaMs", 500)).block();

                StepVerifier.withVirtualTime(() -> simulador.risk("BOG"))
                                .expectSubscription()
                                .thenAwait(Duration.ofMillis(500))
                                .expectNext(30)
                                .verifyComplete();
        }
}
