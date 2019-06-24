package com.spark.last;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import sparkservice.sparksql;


@RestController
@SpringBootApplication
public class LastApplication {

    public static void main(String[] args) {
        SpringApplication.run(LastApplication.class, args);
    }


    @GetMapping("/hello")
    public String hello(){
        sparksql helloScala = new sparksql();
        helloScala.sayHello( " my first scala!");
        return "hello";
    }

}
