package com.spark.last.service;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author: zjf
 * @Date: 2019/6/26 20:00
 * @Description:
 */
public class SparkServiceTest {

    @Test
    public void getAgeData() {

        SparkService sparkService=new SparkService();
        sparkService.getAgeData();
    }
}