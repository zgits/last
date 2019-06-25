package com.spark.last.service;

import com.spark.last.model.Result;
import org.apache.spark.sql.Row;
import sparkservice.sparksql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author: zjf
 * @Date: 2019/6/23 23:27
 * @Description:
 */
public class SparkService {
    public static void main(String[] args) {


        /**
         * 基本想法是：首先是根据用户表，查询所有的用户信息，之后再根据userid查询打分表中用户的打分信息，
         * 根据打分信息中的电影id，查询电影的信息（过滤掉没有分类的电影），查询电影信息之后，
         * 对这些有打分信息的电影提取它们的分类，分类提取之后，查看某个分类（类型）有多少电影，之后再去统计这些电影的打分信息
         * 并根据打分信息查询用户信息，分为男女
         * 总体流程：
         * 查询用户表信息-->查询这些用户的打分信息-->查询打分对应的电影信息-->提取分类-->统计分类下的电影-->查询这些电影的打分信息-->查询打分的用户信息
         */


        sparksql sparksql=new sparksql();


        //获得所有用户信息
        List<Row> userInfoList=sparksql.getUserInfo();

        String userIds="";

        //拼接字符串，为打分信息做准备
        int i=0;
        for (Row row : userInfoList) {
            if(i!=userInfoList.size()-1){
                userIds+=row.get(1)+",";
            }else{
                userIds+=row.get(1);
            }
            i++;

        }

//        获取打分信息
        List<Row> ratingsList=sparksql.getRatingInfo(userIds);

        Set<Integer> movieIds=new HashSet<>();

        for (Row row : ratingsList) {
            movieIds.add((Integer) row.get(2));
        }

        String moviesId="";

        i=0;

//        拼接打分的电影的id字符串
        for (Integer movieId : movieIds) {

            if(i!=movieIds.size()-1){
                moviesId+=movieId+",";
            }else{
                moviesId+=movieId;
            }
            i++;

        }

//        获取有用户打分的电影信息
        List<Row> moviesList=sparksql.getMovieInfo(moviesId);

        Set<String> types=new HashSet<>();

        for (Row row : moviesList) {
            String type= (String) row.get(3);
            System.out.println(type);

            String[] temp=type.split("\\|");
            System.out.println("***********");
            System.out.println(temp[0]);
            System.out.println("***********");
            for (String s : temp) {
                types.add(s);
            }
        }

        System.out.println("类型合集");
        for (String type : types) {
            System.out.println(type);
        }

//        System.out.println(types.);



//        System.out.println(moviesList.size());
//
//
//        System.out.println(moviesId);

        //System.out.println(ratingsList.size());



    }


    public Result getSexData() {


        /**
         * 基本想法是：首先是根据用户表，查询所有的用户信息，之后再根据userid查询打分表中用户的打分信息，
         * 根据打分信息中的电影id，查询电影的信息（过滤掉没有分类的电影），查询电影信息之后，
         * 对这些有打分信息的电影提取它们的分类，分类提取之后，查看某个分类（类型）有多少电影，之后再去统计这些电影的打分信息
         * 并根据打分信息查询用户信息，分为男女
         * 总体流程：
         * 查询用户表信息-->查询这些用户的打分信息-->查询打分对应的电影信息-->提取分类-->统计分类下的电影-->查询这些电影的打分信息-->查询打分的用户信息
         */


        sparksql sparksql=new sparksql();


        //获得所有用户信息
        List<Row> userInfoList=sparksql.getUserInfo();

        String userIds="";

        //拼接字符串，为打分信息做准备
        int i=0;
        for (Row row : userInfoList) {
            if(i!=userInfoList.size()-1){
                userIds+=row.get(1)+",";
            }else{
                userIds+=row.get(1);
            }
            i++;

        }

//        获取打分信息
        List<Row> ratingsList=sparksql.getRatingInfo(userIds);

        Set<Integer> movieIds=new HashSet<>();

        for (Row row : ratingsList) {
            movieIds.add((Integer) row.get(2));
        }

        String moviesId="";

        i=0;

//        拼接打分的电影的id字符串
        for (Integer movieId : movieIds) {

            if(i!=movieIds.size()-1){
                moviesId+=movieId+",";
            }else{
                moviesId+=movieId;
            }
            i++;

        }

//        获取有用户打分的电影信息
        List<Row> moviesList=sparksql.getMovieInfo(moviesId);

        Set<String> types=new HashSet<>();

        for (Row row : moviesList) {
            String type= (String) row.get(3);
            System.out.println(type);

            String[] temp=type.split("\\|");
            for (String s : temp) {
                types.add(s);
            }
        }




        Result result=new Result();

        result.setTypes(new ArrayList<>(types));

        return result;



    }
}
