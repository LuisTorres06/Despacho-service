package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

public class CupoInsuficienteException extends DomainException {
    public CupoInsuficienteException(Long vehiculoId, int pesoKg) {
        super("Cupo insuficiente para vehiculo " + vehiculoId + " y peso " + pesoKg, HttpStatus.CONFLICT);
    }
}
