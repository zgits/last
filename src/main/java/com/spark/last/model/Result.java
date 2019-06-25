package com.spark.last.model;

import java.util.List;

/**
 * @Author: zjf
 * @Date: 2019/6/25 23:10
 * @Description:
 */
public class Result {

    List<String> types;//类型的合集

    List<Float> man;//性别男的评分合集


    List<Float> woman;//性别女的评分合集

    public List<String> getTypes() {
        return types;
    }

    public void setTypes(List<String> types) {
        this.types = types;
    }

    public List<Float> getMan() {
        return man;
    }

    public void setMan(List<Float> man) {
        this.man = man;
    }

    public List<Float> getWoman() {
        return woman;
    }

    public void setWoman(List<Float> woman) {
        this.woman = woman;
    }
}
