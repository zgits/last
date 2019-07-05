package com.spark.last.last;

import com.spark.last.model.LastResult;
import last.GetData;
import last.rating;
import org.apache.spark.sql.sources.In;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: zjf
 * @Date: 2019/7/5 17:10
 * @Description:
 */
public class getData {
    GetData get = new GetData();
    List<rating> temp1 = get.test(110);

    Map<String, Integer> tempmap = new HashMap<>();

    LastResult result = new LastResult();

    List<Double> ratings = new ArrayList<>();
    List<String> times = new ArrayList<>();

    DecimalFormat df = new DecimalFormat("#0.0");
    double sum = 0;
    String start = "";
    int count = 0;
    public LastResult getResults() {

        start = stampToDate(String.valueOf(temp1.get(0).getTime()));
        for (int i = 0; i < temp1.size(); i++) {
            rating rating = temp1.get(i);
            if (!start.equals(stampToDate(String.valueOf(rating.getTime())))) {
                times.add(start);
                ratings.add(Double.valueOf(df.format(sum/count)));
                start = stampToDate(String.valueOf(rating.getTime()));
                count = 0;
                sum = 0;
            }
            double rating1 = rating.getRating();
            count++;
            sum += rating1;


        }
        times.add(start);
        ratings.add(Double.valueOf(df.format(sum/count)));
        result.setRatings(ratings);
        result.setTime(times);
        return result;
    }

    public static String stampToDate(String s) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }


}
