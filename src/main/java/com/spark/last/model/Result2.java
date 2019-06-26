package com.spark.last.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: zjf
 * @Date: 2019/6/26 21:38
 * @Description:
 */
public class Result2 {

    List<Float> age=new ArrayList<>();//年龄段评分的合集


    List<String> ageType=new ArrayList<>();

    public List<Float> getAge() {
        return age;
    }

    public void setAge(List<Float> age) {
        this.age = age;
    }

    public List<String> getAgeType() {
        return ageType;
    }

    public void setAgeType(List<String> ageType) {
        this.ageType = ageType;
    }
}
