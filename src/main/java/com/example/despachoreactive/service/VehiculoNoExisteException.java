package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

public class VehiculoNoExisteException extends DomainException {
    public VehiculoNoExisteException(Long id) {
        super("Vehiculo no existe: " + id, HttpStatus.NOT_FOUND);
    }
}
