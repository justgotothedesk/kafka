package com.example.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class TestController {
    @Autowired
    TestService service;

    @GetMapping("/insert")
    public String insert() {
        return service.insert();
    }
}
