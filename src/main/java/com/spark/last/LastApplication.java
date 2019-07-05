package com.spark.last;

import com.spark.last.last.getData;
import com.spark.last.model.LastResult;
import com.spark.last.model.Result;
import com.spark.last.service.SparkService;
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


    @GetMapping("/getSexData")
    public Result hello(){
        SparkService sparkService=new SparkService();
        sparkService.init();
        return sparkService.getSexData();
    }


    @GetMapping("/getLast")
    public LastResult last(){
        getData method=new getData();
        return method.getResults();
    }

}
