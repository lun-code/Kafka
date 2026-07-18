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

        var configs = new HashMap<String, Object>();

        configs.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers()
        );

        configs.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        configs.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JacksonJsonSerializer.class
        );

        // Con un solo broker local, all y 1 se comportan casi igual porque no hay réplicas que esperar
        configs.put(
                ProducerConfig.ACKS_CONFIG, "all"
        );

        // Evita que el serializer añada un header con el nombre completo de la
        // clase Java del productor (str_producer.dto.EventoDTO) — el consumer
        // no tiene esa clase en su classpath, así que confiamos en VALUE_DEFAULT_TYPE
        // del lado del consumer en vez de en este header
        configs.put(
                JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS,
                false
        );

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