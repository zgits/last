package com.spark.last.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import scala.myScala;

@Controller
public class helloWorld {
    @RequestMapping("/hello")
    public String hello(){
        myScala.sayhello("scala");
        return "/pages/login.html";
    }
}
