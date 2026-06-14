package com.kafka_producer.config;

import com.kafka_producer.controller.ProductEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConfig {


    @Value("${spring.kafka.producer.bootstrap.servers}")
    private List<String> bootstrapServers;

    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${spring.kafka.producer.acks}")
    private String acks;

    @Value("${spring.kafka.producer.properties.delivery.timeout.ms}")
    private String deliveryTimeout;

    @Value("${spring.kafka.producer.properties.linger.ms}")
    private String linger;

    @Value("${spring.kafka.producer.properties.request.timeout.ms}")
    private String requestTimeout;

    @Value("${spring.kafka.producer.properties.enabled.idempotence}")
    private Boolean idempotence;

    @Value("${spring.kafka.producer.property,max.in.flight.requests.per.connection}")
    private Integer maxInFlightRequestsPerConnection;

    //Custom Property.....................

    @Bean
    Map<String,Object> producerConfig(){

        Map<String,Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,valueSerializer);
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,maxInFlightRequestsPerConnection);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,deliveryTimeout);
        props.put(ProducerConfig.LINGER_MS_CONFIG,linger);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,requestTimeout);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,idempotence);

        return props;
    }

    //That Producer Factory Will use our custom Configuration to create Object//////
    @Bean
    ProducerFactory<String, ProductEvent> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    KafkaTemplate<String,ProductEvent> kafkaTemplate(){
        return new KafkaTemplate<String,ProductEvent>(producerFactory());
    }


    @Bean
    NewTopic newTopic(){
        return TopicBuilder.name("product-created-event-topic")
                .partitions(3)
                .replicas(3)
                .configs(Map.of("min-in-ync-replicas" ,"2"))
                .build();
    }
}
