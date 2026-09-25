package com.example.despachoreactive.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.List;

public record DespachoRequest(
        @NotNull Long clienteId,
        @NotNull String ciudad,
        @NotEmpty List<@Valid PaqueteRequest> paquetes) {
    public record PaqueteRequest(@NotNull Long vehiculoId, @NotNull @Positive Integer pesoKg) {}
}
