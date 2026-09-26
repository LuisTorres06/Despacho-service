package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

public class DespachoNoExisteException extends DomainException {
    public DespachoNoExisteException(Long id) {
        super("Despacho no existe: " + id, HttpStatus.NOT_FOUND);
    }
}
