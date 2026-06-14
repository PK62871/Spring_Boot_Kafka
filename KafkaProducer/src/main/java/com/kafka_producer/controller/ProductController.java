package com.kafka_producer.controller;

import com.kafka_producer.exception.ErrorMessage;
import com.kafka_producer.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.http.HttpResponse;
import java.util.Date;

@RestController
@RequestMapping("/products")
public class ProductController {

    private static final Logger Logger =  LoggerFactory.getLogger(ProductController.class);

    private final ProductService productService;

    ProductController(ProductService productService){
        this.productService = productService;
    }


    @PostMapping("/create")
    public ResponseEntity<Object> createProduct(@RequestBody CreateProductRequest request){

        String productId = null;
        try{
             productId = productService.createProduct(request);

        }catch (Exception e){
            Logger.info(e.getMessage(),e);

    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(new ErrorMessage(new Date(),e.getMessage(),"/create"));
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }
}
