package com.spark.last.model;

import java.util.List;

/**
 * @Author: zjf
 * @Date: 2019/7/5 17:13
 * @Description:
 */
public class LastResult {

    List<Double> ratings;

    List<String> time;

    public List<Double> getRatings() {
        return ratings;
    }

    public void setRatings(List<Double> ratings) {
        this.ratings = ratings;
    }

    public List<String> getTime() {
        return time;
    }

    public void setTime(List<String> time) {
        this.time = time;
    }
}
