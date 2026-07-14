package com.example.str_producer.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;

@Configuration // Indica a Spring que esta clase contiene configuración y Beans
public class KafkaAdminConfig {

    @Autowired
    private KafkaProperties kafkaProperties;
    // Lee las propiedades de Kafka desde application.properties

    @Bean
    public KafkaAdmin kafkaAdmin() {
        var configs = new HashMap<String, Object>();

        // Configura el broker al que se conectará el administrador de Kafka
        // Equivale a: spring.kafka.producer.bootstrap-servers=localhost:9092
        configs.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers()
        );

        // Bean que permite a Spring administrar Kafka (crear topics, etc.)
        return new KafkaAdmin(configs);
    }

    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("str-topic") // Nombre del topic
                        .partitions(2) // Número de particiones
                        .replicas(1) // Copias por partición (1 broker)
                        .build()
        );
    }
}