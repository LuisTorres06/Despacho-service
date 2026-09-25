package com.example.despachoreactive.controller;

import com.example.despachoreactive.dto.VehiculoRequest;
import com.example.despachoreactive.service.VehiculoNoExisteException;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/vehiculos")
public class VehiculoController {
    private final DatabaseClient db;

    public VehiculoController(DatabaseClient db) {
        this.db = db;
    }

    @GetMapping
    public Flux<VehiculoRequest> listar() {
        return db.sql("SELECT id, placa, ciudad, cupo_kg FROM vehiculo ORDER BY id").map((r, m) -> new VehiculoRequest(r.get("id", Long.class), r.get("placa", String.class), r.get("ciudad", String.class), r.get("cupo_kg", Integer.class))).all();
    }

    @PostMapping
    public Mono<VehiculoRequest> crear(@Valid @RequestBody VehiculoRequest request) {
        return upsert(request);
    }

    @GetMapping("/{id}")
    public Mono<VehiculoRequest> buscar(@PathVariable Long id) {
        return db.sql("SELECT id, placa, ciudad, cupo_kg FROM vehiculo WHERE id = :id")
                .bind("id", id).map((r, m) -> mapear(r)).one()
                .switchIfEmpty(Mono.error(new VehiculoNoExisteException(id)));
    }

    @PutMapping("/{id}")
    public Mono<VehiculoRequest> actualizar(@PathVariable Long id, @Valid @RequestBody VehiculoRequest request) {
        return db.sql("UPDATE vehiculo SET placa = :placa, ciudad = :ciudad, cupo_kg = :cupo WHERE id = :id RETURNING id, placa, ciudad, cupo_kg")
                .bind("id", id).bind("placa", request.placa()).bind("ciudad", request.ciudad()).bind("cupo", request.cupoKg())
                .map((r, m) -> mapear(r)).one().switchIfEmpty(Mono.error(new VehiculoNoExisteException(id)));
    }

    @DeleteMapping("/{id}")
    public Mono<Void> eliminar(@PathVariable Long id) {
        return db.sql("DELETE FROM vehiculo WHERE id = :id").bind("id", id).fetch().rowsUpdated()
                .flatMap(rows -> rows == 0 ? Mono.error(new VehiculoNoExisteException(id)) : Mono.empty());
    }

    @PostMapping(value = "/bulk", consumes = MediaType.APPLICATION_NDJSON_VALUE)
    public Mono<?> bulk(@Valid @RequestBody Flux<VehiculoRequest> requests) {
        return requests.buffer(500).concatMap(lote -> Flux.fromIterable(lote).concatMap(this::upsert).count()).reduce(0L, Long::sum).map(total -> java.util.Map.of("procesados", total));
    }

    private Mono<VehiculoRequest> upsert(VehiculoRequest request) {
        return db.sql("INSERT INTO vehiculo (id, placa, ciudad, cupo_kg) VALUES (:id, :placa, :ciudad, :cupo) ON CONFLICT (id) DO UPDATE SET placa = EXCLUDED.placa, ciudad = EXCLUDED.ciudad, cupo_kg = EXCLUDED.cupo_kg RETURNING id, placa, ciudad, cupo_kg")
                .bind("id", request.id()).bind("placa", request.placa()).bind("ciudad", request.ciudad()).bind("cupo", request.cupoKg()).map((r, m) -> mapear(r)).one();
    }

    private VehiculoRequest mapear(io.r2dbc.spi.Readable row) {
        return new VehiculoRequest(row.get("id", Long.class), row.get("placa", String.class), row.get("ciudad", String.class), row.get("cupo_kg", Integer.class));
    }
}
