package com.example.despachoreactive.dto;

import java.time.Instant;

public record DespachoEvent(Long despachoId, String estado, String mensaje, String trazaId, Instant instante) {}
