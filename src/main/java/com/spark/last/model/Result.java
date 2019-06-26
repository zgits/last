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

    Result2 result2;

    Result3 result3;

    public Result3 getResult3() {
        return result3;
    }

    public void setResult3(Result3 result3) {
        this.result3 = result3;
    }

    public Result2 getResult2() {
        return result2;
    }

    public void setResult2(Result2 result2) {
        this.result2 = result2;
    }

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
