package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class TestService {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    public String insert() {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("test", null, LocalDateTime.now().toString());
        future.whenComplete((success, failure) -> {
            if (success != null) {
                log.info("[Success] partition : {}, offset : {}", success.getRecordMetadata().offset(), success.getRecordMetadata().partition());
            } else {
                log.error("[Failure] error msg : {}", failure.getMessage());
            }
        });

        return "{}";
    }
    @KafkaListener (topics = "test", groupId = "myGroup1" )
    public void consume(String message) throws InterruptedException {
        log.info("[CONSUMER] message: {}", message);
        Thread.sleep(5000L);
        log.info("End of sleep");
    }
}
