package com.example.despachoreactive.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record DespachoResponse(Long id, Long clienteId, String ciudad, String estado,
                                BigDecimal tarifa, BigDecimal total, Integer scoreRiesgo,
                                String trazaId, Instant creadoEn, Instant expiraEn,
                                List<PaqueteResponse> paquetes) {
    public record PaqueteResponse(Long id, Long despachoId, Long vehiculoId, Integer pesoKg) {}
}
