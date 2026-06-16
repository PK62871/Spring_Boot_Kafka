package com.kafka_producer.service;

import com.core.module.ProductEvent;
import com.kafka_producer.controller.CreateProductRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ProductServiceImpl implements  ProductService{

private static final Logger LOGGER = LoggerFactory.getLogger(ProductServiceImpl.class);

    private final KafkaTemplate<String,ProductEvent> kafkaTemplate;

    public ProductServiceImpl(KafkaTemplate<String,ProductEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductRequest request) throws Exception{


        String productId = UUID.randomUUID().toString();

        ProductEvent productEvent = new ProductEvent(productId,request.getTitle(),request.getPrice(),request.getQuantity());

        /*
        This Will Send Message to kafka as Asynchronously
        Without getting acks it will send message...So data loss chance is there
         */

//        CompletableFuture<SendResult<String, ProductEvent>> future = kafkaTemplate.send("product-created-event-topic", productId, productEvent);
//        future.whenComplete((result,exception)->{
//            if(exception != null){
//                LOGGER.error("*********Failed to send product event {}",exception.getMessage());
//            }else {
//            LOGGER.info("******Successfully send product event {}",result.getRecordMetadata().offset());
//
//            }
//        });

        LOGGER.info("Before Publishing a ProductCreatedEvent..");

        SendResult<String, ProductEvent> result = kafkaTemplate.send("product-created-event-topic", productId, productEvent).get();

        /*
        Print MetaData like Partition,topicName,Offset
         */

        LOGGER.info("Partition {}", result.getRecordMetadata().partition());
        LOGGER.info("Topic {}",result.getRecordMetadata().topic());
        LOGGER.info("Offset {}",result.getRecordMetadata().offset());

        LOGGER.info("**********Successfully send product event {}",productId);
        return productId;
    }
}
