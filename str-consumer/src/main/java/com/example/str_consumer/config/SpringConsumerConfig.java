package com.example.str_consumer.config;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.HashMap;

@Log4j2
@Configuration // Le dice a Spring que esta clase define Beans que debe gestionar
public class SpringConsumerConfig {

    @Autowired
    private KafkaProperties kafkaProperties;
    // Contiene la configuración de Kafka leída desde application.properties
    // Ejemplo:
    // spring.kafka.bootstrap-servers=localhost:29092


    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

        // Mapa con la configuración que necesita el consumidor Kafka
        var configs = new HashMap<String, Object>();

        // Dirección del broker Kafka al que se conectará el consumidor
        configs.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers()
        );

        // Convierte la clave del mensaje de bytes -> String al leerla desde Kafka
        configs.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        // Convierte el valor del mensaje de bytes -> String al leerlo desde Kafka
        configs.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        // Crea la fábrica encargada de crear consumidores Kafka configurados
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> validMessageContainerFactory(ConsumerFactory<String, String> consumerFactory){
        // Factory que usan los métodos anotados con @KafkaListener
        // para crear y gestionar los hilos/contenedores que escuchan Kafka
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRecordInterceptor(validMessage());
        return factory;
    }

    private RecordInterceptor<String, String> validMessage(){
        return (record, consumer) -> {
            if(record.value().contains("MUNDO")){
                log.info("Contiene la palabra MUNDO");
                return record;
            }
            return record;
        };
    }
}