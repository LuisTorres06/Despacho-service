package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

public class DomainException extends RuntimeException {
    private final HttpStatus status;

    public DomainException(String message, HttpStatus status) {
        super(message);
        this.status = status;
    }

    public HttpStatus status() {
        return status;
    }
}
