package com.example.str_producer.controller;

import com.example.str_producer.service.StringProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer")
public class StringProducerController {

    @Autowired
    private StringProducerService stringProducerService;

    @PostMapping
    public ResponseEntity<String> sendMessage(@RequestBody String message) {
        stringProducerService.sendMessage(message);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }
}
