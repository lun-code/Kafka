package com.example.str_producer.config;

import com.example.str_producer.dto.EventoDTO;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;

@Configuration // Clase de configuración donde definimos Beans que Spring gestionará
public class StringProducerFactoryConfig {

    @Autowired
    private KafkaProperties kafkaProperties;
    // Contiene la configuración de Kafka leída desde application.properties
    // Ejemplo:
    // spring.kafka.producer.bootstrap-servers=localhost:9092


    @Bean
    public ProducerFactory<String, EventoDTO> producerFactory() {

        // Mapa con la configuración que necesita el productor Kafka
        var configs = new HashMap<String, Object>();

        // Dirección del broker Kafka al que se conectará el productor
        configs.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers()
        );

        // Convierte la clave del mensaje String -> bytes antes de enviarla a Kafka
        configs.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        // Convierte el valor del mensaje (EventoDTO) -> JSON antes de enviarlo a Kafka
        configs.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JacksonJsonSerializer.class
        );

        // Crea la fábrica encargada de crear productores Kafka configurados
        return new DefaultKafkaProducerFactory<>(configs);
    }


    @Bean
    public KafkaTemplate<String, EventoDTO> kafkaTemplate() {

        // KafkaTemplate es el objeto que usará la aplicación para enviar mensajes:
        //
        // kafkaTemplate.send("str-topic", evento.userId(), evento);
        //
        // Internamente utiliza la ProducerFactory para obtener productores Kafka.
        return new KafkaTemplate<>(producerFactory());
    }
}