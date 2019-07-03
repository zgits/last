package com.spark.last.controller;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import scala.searchMovies;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/movie")
public class movieController {
    @RequestMapping("/searchMovies")
    @ResponseBody
    public List<String> search(HttpServletRequest request, HttpServletResponse response){
        JavaRDD list = searchMovies.search(request.getParameter("movieName"));
        List<String> fin = list.collect();

        return fin;
    }
}
