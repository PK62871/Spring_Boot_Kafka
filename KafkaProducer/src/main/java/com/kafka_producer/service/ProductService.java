package com.kafka_producer.service;

import com.kafka_producer.controller.CreateProductRequest;

public interface ProductService {

    public String createProduct(CreateProductRequest request)throws Exception;
}
