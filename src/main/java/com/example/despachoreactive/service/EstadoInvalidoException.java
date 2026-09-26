package com.example.despachoreactive.service;


import org.springframework.http.HttpStatus;

public class EstadoInvalidoException extends DomainException {
    public EstadoInvalidoException(String estadoActual, String esperado) {
        super("Estado invalido: actual=" + estadoActual + ", esperado=" + esperado, HttpStatus.CONFLICT);
    }
}
