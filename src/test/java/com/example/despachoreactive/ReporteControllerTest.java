package com.example.despachoreactive;

import com.example.despachoreactive.controller.ReporteController;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReporteControllerTest {
    @Test
    void acumuladorConservaLosResultadosPorCiudad() {
        Map<String, Object> bogota = Map.of("ciudad", "BOG", "kilos", 10L, "valor", BigDecimal.TEN);
        Map<String, Object> medellin = Map.of("ciudad", "MDE", "kilos", 20L, "valor", BigDecimal.ONE);

        StepVerifier.create(Flux.just(bogota, medellin).scan(new java.util.LinkedHashMap<String, Object>(), ReporteController::acumular).skip(2))
                .assertNext(acumulado -> {
                    Map<?, ?> ciudades = (Map<?, ?>) acumulado.get("ciudades");
                    assertTrue(ciudades.containsKey("BOG"));
                    assertTrue(ciudades.containsKey("MDE"));
                    assertThat(ciudades.get("BOG")).isEqualTo(bogota);
                })
                .verifyComplete();
    }

    @Test
    void acumuladorReemplazaLaVersionDeUnaCiudadSinPerderLasDemas() {
        Map<String, Object> anterior = Map.of("ciudad", "BOG", "kilos", 10L, "valor", BigDecimal.TEN);
        Map<String, Object> actualizado = Map.of("ciudad", "BOG", "kilos", 15L, "valor", BigDecimal.valueOf(15));
        Map<String, Object> resultado = ReporteController.acumular(ReporteController.acumular(new java.util.LinkedHashMap<>(), anterior), actualizado);

        Map<?, ?> ciudades = (Map<?, ?>) resultado.get("ciudades");
        assertThat(ciudades).hasSize(1);
        assertThat(ciudades.get("BOG")).isEqualTo(actualizado);
    }
}