package com.spark.last.model;

import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: zjf
 * @Date: 2019/6/26 22:31
 * @Description:
 */
public class Result3 {

    List<String> work=new ArrayList<>();

    List<Float> workScore=new ArrayList<>();

    public List<String> getWork() {
        return work;
    }

    public void setWork(List<String> work) {
        this.work = work;
    }

    public List<Float> getWorkScore() {
        return workScore;
    }

    public void setWorkScore(List<Float> workScore) {
        this.workScore = workScore;
    }
}
