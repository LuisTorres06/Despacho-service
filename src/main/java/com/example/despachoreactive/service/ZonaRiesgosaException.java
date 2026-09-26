package com.example.despachoreactive.service;

import org.springframework.http.HttpStatus;

public class ZonaRiesgosaException extends DomainException {

    public ZonaRiesgosaException(int score) {
        super("Zona riesgosa. score=" + score, HttpStatus.UNPROCESSABLE_ENTITY);
    }
}
