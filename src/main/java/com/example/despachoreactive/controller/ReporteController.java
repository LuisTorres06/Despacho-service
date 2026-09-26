package com.example.despachoreactive.controller;

import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/reports")
public class ReporteController {
    private final DatabaseClient db;

    public ReporteController(DatabaseClient db) {
        this.db = db;
    }

    @GetMapping("/ciudades")
    public Flux<Map<String, Object>> ciudades() {
        return db.sql("""
                        SELECT d.ciudad,
                               COALESCE(SUM(p.kilos), 0) kilos,
                               COALESCE(SUM(d.total), 0) valor
                        FROM despacho d
                        JOIN (
                            SELECT despacho_id, SUM(peso_kg) kilos
                            FROM paquete
                            GROUP BY despacho_id
                        ) p ON p.despacho_id = d.id
                        GROUP BY d.ciudad
                        ORDER BY d.ciudad
                        """)
                .map((r, m) -> Map.<String, Object>of(
                        "ciudad", r.get("ciudad", String.class),
                        "kilos", r.get("kilos", Long.class),
                        "valor", r.get("valor", BigDecimal.class)))
                .all()
                // controla la demanda downstream en lotes
                .limitRate(100);
    }

    @GetMapping(value = "/ciudades/stream", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<Map<String, Object>> stream() {
        return ciudades()
                // acumulado incremental en tiempo real
                .scan(new LinkedHashMap<String, Object>(), ReporteController::acumular);
    }

    public static Map<String, Object> acumular(Map<String, Object> acumulado, Map<String, Object> fila) {
        Map<String, Object> resultado = new LinkedHashMap<>(acumulado);
        String ciudad = (String) fila.get("ciudad");

        Map<String, Object> ciudades = new LinkedHashMap<>();
        Object existentes = resultado.get("ciudades");
        if (existentes instanceof Map<?, ?> mapa) {
            mapa.forEach((clave, valor) -> ciudades.put(String.valueOf(clave), valor));
        }

        ciudades.put(ciudad, fila);
        resultado.put("ciudades", ciudades);
        return resultado;
    }
}