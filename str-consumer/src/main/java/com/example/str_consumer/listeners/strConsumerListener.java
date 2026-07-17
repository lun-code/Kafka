package com.example.str_consumer.listeners;

import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class strConsumerListener {

    @KafkaListener(groupId = "group-1", topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"0"}), containerFactory = "strContainerFactory")
    public void listener(String message){
        log.info("LISTENER 1 ::: Recibiendo un mensaje {}", message);
    }

    @KafkaListener(groupId = "group-1", topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"1"}), containerFactory = "strContainerFactory")
    public void listener2(String message){
        log.info("LISTENER 2 ::: Recibiendo un mensaje {}", message);
    }

    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "strContainerFactory")
    public void listener3(String message){
        log.info("LISTENER 3 ::: Recibiendo un mensaje {}", message);
    }
}
