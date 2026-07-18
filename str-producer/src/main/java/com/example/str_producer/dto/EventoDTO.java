package com.example.str_producer.dto;

import java.time.Instant;

public record EventoDTO(

        String userId,
        String eventType,
        String page,
        Instant timestamp
) {}