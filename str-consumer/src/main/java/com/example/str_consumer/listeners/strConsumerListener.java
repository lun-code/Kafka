package com.example.str_consumer.listeners;

import com.example.str_consumer.dto.EventoDTO;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class strConsumerListener {

    @KafkaListener(groupId = "group-1", topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"0"}), containerFactory = "validMessageContainerFactory")
    public void listener(EventoDTO evento){
        log.info("LISTENER 1 ::: Recibiendo evento {}", evento);
    }

    @KafkaListener(groupId = "group-1", topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"1"}), containerFactory = "validMessageContainerFactory")
    public void listener2(EventoDTO evento){
        log.info("LISTENER 2 ::: Recibiendo evento {}", evento);
    }

    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "validMessageContainerFactory")
    public void listener3(EventoDTO evento){
        log.info("LISTENER 3 ::: Recibiendo evento {}", evento);
    }
}
