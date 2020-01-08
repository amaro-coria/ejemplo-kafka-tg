package com.tecgurus.demo.kafka.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/v1/demo")
public class DemoController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private String topic = "Topic1";

    @GetMapping(value = "/ejemplo/{mensaje}")
    public String demo(@PathVariable String mensaje){
        kafkaTemplate.send(topic, mensaje);
        return "OK";
    }

}
