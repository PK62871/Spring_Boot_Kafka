package com.kafka.consumer.listner;

import com.core.module.ProductEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "product-created-event-topic")
public class ProductsCreatedEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProductsCreatedEventHandler.class);

    @KafkaHandler
    public void handle(ProductEvent productCreatedEvent){

        LOGGER.info("Received event {}", productCreatedEvent);
    }
}
