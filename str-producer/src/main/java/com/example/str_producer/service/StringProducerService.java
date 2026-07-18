package com.example.str_producer.service;

import com.example.str_producer.dto.EventoDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class StringProducerService {

    @Autowired
    private KafkaTemplate<String, EventoDTO> kafkaTemplate;

    public void sendMessage(EventoDTO evento) {
        kafkaTemplate.send("str-topic", evento.userId(), evento).whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Error al enviar el mensaje: {}", exception.getMessage());
                return;
            }
            log.info("Mensaje enviado exitosamente: {}", result.getProducerRecord().value());
            log.info("Partición {}, offset {}", result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        });
    }
}
