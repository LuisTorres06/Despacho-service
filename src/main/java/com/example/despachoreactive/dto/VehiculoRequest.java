package com.example.despachoreactive.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

public record VehiculoRequest(@NotNull Long id, @NotBlank String placa, @NotBlank String ciudad, @PositiveOrZero int cupoKg) {}
