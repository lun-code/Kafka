package com.example.str_producer.dto;

public record EventoDTO(

        String userId,
        String eventType,
        String page,
        String timestamp
) {}