package com.example.kafka;

import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@AllArgsConstructor
public class KafkaConfig {
    final ConsumerFactory<String, String> consumerFactory;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(5); // Number of thread

        ContainerProperties properties = factory.getContainerProperties();
        properties.setAckMode(ContainerProperties.AckMode.MANUAL); // 잘 수신했다고 알려주는 부분

        return factory;
    }
}
