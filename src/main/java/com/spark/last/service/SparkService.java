package com.spark.last.service;

import com.spark.last.model.Result;
import com.spark.last.model.Result2;
import com.spark.last.model.Result3;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.In;
import sparkservice.sparksql;
import sun.nio.cs.ext.IBM037;

import java.text.DecimalFormat;
import java.util.*;

/**
 * @Author: zjf
 * @Date: 2019/6/23 23:27
 * @Description:
 */
public class SparkService {



    sparksql sparksql = new sparksql();


    List<Row> userInfoList;

    List<Row> ratingsList;

    List<Row> moviesList;


    HashMap<Integer, Row> userMap = new HashMap<>();


    HashMap<Integer, List<Row>> ratingsMap = new HashMap<>();


    HashMap<Integer,List<Double>> userRatingMap=new HashMap<>();

    DecimalFormat df = new DecimalFormat("#0.0");

    public void init(){

        long startTime = System.currentTimeMillis();
        //获得所有用户信息
        userInfoList = sparksql.getUserInfo();

        String userIds = "";

        //拼接字符串，为打分信息做准备
        int i = 0;
        for (Row row : userInfoList) {

            userMap.put((Integer) row.get(1), row);

            if (i != userInfoList.size() - 1) {
                userIds += row.get(1) + ",";
            } else {
                userIds += row.get(1);
            }
            i++;

        }

        //获取打分信息
        ratingsList = sparksql.getRatingInfo(userIds);

        List<Integer> movieIds = new ArrayList<>();

        for (Row row : ratingsList) {

            movieIds.add((Integer) row.get(2));
        }
        String moviesId = "";


        long startTime1 = System.currentTimeMillis();
        int start=0;
        for (Row row : userInfoList) {
            List<Double> theSameRating=new ArrayList<>();

            for (int i1 = start; i1 < ratingsList.size(); i1++) {

                Row row1=ratingsList.get(i1);
                if(row.getInt(1)==row1.getInt(1)){
                    theSameRating.add((Double) row1.get(3));
                    start++;
                }else{
                    break;
                }
            }

            userRatingMap.put(row.getInt(1),theSameRating);
        }

        long endTime1 = System.currentTimeMillis();    //获取结束时间

        System.out.println("设置userRatingMap时间：" + (endTime1 - startTime1) + "ms");    //输出程序运行时间


        long startTime2 = System.currentTimeMillis();
        List<Row> tempRatingsList=new ArrayList<>(ratingsList);

        Set<Integer> tempmovieIds = new HashSet<>(movieIds);
        //拼接打分的电影的id字符串

        i = 0;
        start=0;
        for (Integer movieId : tempmovieIds) {
            List<Row> theSameMovie = new ArrayList<>();
            for (Row row : tempRatingsList) {
                if (row.get(2) == movieId) {
                    theSameMovie.add(row);
                }
            }

            ratingsMap.put(movieId, theSameMovie);

            if (i != tempmovieIds.size() - 1) {
                moviesId += movieId + ",";
            } else {
                moviesId += movieId;
            }
            i++;

        }
        long endTime2 = System.currentTimeMillis();    //获取结束时间

        System.out.println("设置ratingsMap时间：" + (endTime2 - startTime2) + "ms");    //输出程序运行时间


        //获取有用户打分的电影信息
        moviesList= sparksql.getMovieInfo(moviesId);

        long endTime = System.currentTimeMillis();    //获取结束时间

        System.out.println("获取数据时间：" + (endTime - startTime) + "ms");    //输出程序运行时间
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

        long startTime = System.currentTimeMillis();
        List<Float> man = new ArrayList<>();//性别为男的用户的平均分合集

        List<Float> woman = new ArrayList<>(); //性别为女的用户的平均分合集


        List<Float> age=new ArrayList<>();//年龄段评分的合集


        //类型的集合
        Set<String> types = new HashSet<>();

        for (Row row : moviesList) {

            String type = (String) row.get(3);
            String[] temp = type.split("\\|");
            for (String s : temp) {
                types.add(s);
            }
        }



        for (String type : types) {

            float mansum = 0;

            float womansum = 0;

            int countman = 0;

            int countwoman = 0;


            //记录该类型的电影id
            List<Integer> typeids = new ArrayList<>();
            for (Row row : moviesList) {
                String temp = (String) row.get(3);
                if (-1 != temp.indexOf(type)) {
                    typeids.add((Integer) row.get(1));
                }
            }

            if (type.equals("Western")) {
                System.out.println("Western类型的打分数量" + typeids.size());
            }
            //查询这些电影的打分情况
            for (Integer typeid : typeids) {

                List<Row> tempRatingsList1 = ratingsMap.get(typeid);

                for (Row row : tempRatingsList1) {

                    Integer userId = (Integer) row.get(1);

                    double rating = (double) row.get(3);

                    //根据打分的人的id去查询个人信息
                    Row row1 = userMap.get(userId);

                    if (row1.get(2).equals("M")) {
                        countman++;
                        mansum += rating;
                    } else {
                        countwoman++;
                        womansum += rating;
                    }


                }
            }



            System.out.println("类型：" + type + "男" + Float.valueOf(df.format(mansum / countman)) + "  女：" + Float.valueOf(df.format(womansum / countwoman)));

            try {
                man.add(Float.valueOf(df.format(mansum / countman)));
                woman.add(Float.valueOf(df.format(womansum / countwoman)));
            }catch (Exception e){
                System.out.println(mansum+"  "+countman);
            }


        }

        Result2 result2=getAgeData();

        Result3 result3=getUserWork();


        Result result = new Result();

        result.setTypes(new ArrayList<>(types));

        result.setMan(man);

        result.setWoman(woman);

        long endTime = System.currentTimeMillis();    //获取结束时间

        System.out.println("程序运行时间1：" + (endTime - startTime) + "ms");    //输出程序运行时间

        result.setResult2(result2);

        result.setResult3(result3);

        return result;


    }



    public Result2 getAgeData(){
        long startTime = System.currentTimeMillis();
        List<Float> age=new ArrayList<>();//年龄段评分的合集


        List<String> ageType=new ArrayList<>();

        List<Row> ageTypeTemp=sparksql.getUserAge();

        for (Row row : ageTypeTemp) {

            switch ((Integer) row.get(0)){
                case 1:ageType.add("18岁以下");break;
                case 56:ageType.add("56岁以上");break;
                case 25:ageType.add("25-34");break;
                case 45:ageType.add("45-49");break;
                case 50:ageType.add("50-55");break;
                case 35:ageType.add("35-44");break;
                case 18:ageType.add("18-24");break;
            }
        }


        List<Integer> userId1=new ArrayList<>();//七个年龄段合集
        List<Integer> userId56=new ArrayList<>();
        List<Integer> userId25=new ArrayList<>();
        List<Integer> userId45=new ArrayList<>();
        List<Integer> userId50=new ArrayList<>();
        List<Integer> userId35=new ArrayList<>();
        List<Integer> userId18=new ArrayList<>();


        for (Row row : userInfoList) {

            switch ((Integer) row.getInt(3)){
                case 1:userId1.add(row.getInt(1));break;
                case 56:userId56.add(row.getInt(1));break;
                case 25:userId25.add(row.getInt(1));break;
                case 45:userId45.add(row.getInt(1));break;
                case 50:userId50.add(row.getInt(1));break;
                case 35:userId35.add(row.getInt(1));break;
                case 18:userId18.add(row.getInt(1));break;
            }
        }

        age.add(Float.valueOf(df.format(average(userId1))));
        age.add(Float.valueOf(df.format(average(userId18))));
        age.add(Float.valueOf(df.format(average(userId25))));
        age.add(Float.valueOf(df.format(average(userId35))));
        age.add(Float.valueOf(df.format(average(userId45))));
        age.add(Float.valueOf(df.format(average(userId50))));
        age.add(Float.valueOf(df.format(average(userId56))));


        Result2 result2=new Result2();
        result2.setAgeType(ageType);
        result2.setAge(age);


        long endTime = System.currentTimeMillis();    //获取结束时间

        System.out.println("程序运行时间2：" + (endTime - startTime) + "ms");    //输出程序运行时间
        return result2;


    }

    public Result3 getUserWork(){

        long startTime = System.currentTimeMillis();
        List<Row> workTemp=sparksql.getUserWork();

        List<Float> workScore=new ArrayList<>();

        List<String> work=new ArrayList<>();

        List<Integer> userId0=new ArrayList<>();
        List<Integer> userId1=new ArrayList<>();
        List<Integer> userId2=new ArrayList<>();
        List<Integer> userId3=new ArrayList<>();
        List<Integer> userId4=new ArrayList<>();
        List<Integer> userId5=new ArrayList<>();
        List<Integer> userId6=new ArrayList<>();
        List<Integer> userId7=new ArrayList<>();
        List<Integer> userId8=new ArrayList<>();
        List<Integer> userId9=new ArrayList<>();
        List<Integer> userId10=new ArrayList<>();
        List<Integer> userId11=new ArrayList<>();
        List<Integer> userId12=new ArrayList<>();
        List<Integer> userId13=new ArrayList<>();
        List<Integer> userId14=new ArrayList<>();
        List<Integer> userId15=new ArrayList<>();
        List<Integer> userId16=new ArrayList<>();
        List<Integer> userId17=new ArrayList<>();
        List<Integer> userId18=new ArrayList<>();
        List<Integer> userId19=new ArrayList<>();
        List<Integer> userId20=new ArrayList<>();




        for (Row row : workTemp) {
            switch ((Integer)row.get(0)){
                case 0:work.add("其他/未指定");break;
                case 1:work.add("学术/教育者");break;
                case 2:work.add("艺术家");break;
                case 3:work.add("文书/行政");break;
                case 4:work.add("大学/研究生");break;
                case 5:work.add("客户服务");break;
                case 6:work.add("医生/保健");break;
                case 7:work.add("行政/管理");break;
                case 8:work.add("农民");break;
                case 9:work.add("家庭主妇");break;
                case 10:work.add("学生");break;
                case 11:work.add("律师");break;
                case 12:work.add("程序员");break;
                case 13:work.add("退休");break;
                case 14:work.add("销售/营销");break;
                case 15:work.add("科学家");break;
                case 16:work.add("个体经营");break;
                case 17:work.add("技术员/工程师");break;
                case 18:work.add("商人/工匠");break;
                case 19:work.add("失业");break;
                case 20:work.add("作者");break;
            }
        }

        for (Row row : userInfoList) {
            switch ((Integer) row.getInt(4)){
                case 0:userId0.add(row.getInt(1));break;
                case 1:userId1.add(row.getInt(1));break;
                case 2:userId2.add(row.getInt(1));break;
                case 3:userId3.add(row.getInt(1));break;
                case 4:userId4.add(row.getInt(1));break;
                case 5:userId5.add(row.getInt(1));break;
                case 6:userId6.add(row.getInt(1));break;
                case 7:userId7.add(row.getInt(1));break;
                case 8:userId8.add(row.getInt(1));break;
                case 9:userId9.add(row.getInt(1));break;
                case 10:userId10.add(row.getInt(1));break;
                case 11:userId11.add(row.getInt(1));break;
                case 12:userId12.add(row.getInt(1));break;
                case 13:userId13.add(row.getInt(1));break;
                case 14:userId14.add(row.getInt(1));break;
                case 15:userId15.add(row.getInt(1));break;
                case 16:userId16.add(row.getInt(1));break;
                case 17:userId17.add(row.getInt(1));break;
                case 18:userId18.add(row.getInt(1));break;
                case 19:userId19.add(row.getInt(1));break;
                case 20:userId20.add(row.getInt(1));break;
            }
        }


        workScore.add(Float.valueOf(df.format(average(userId0))));
        workScore.add(Float.valueOf(df.format(average(userId1))));
        workScore.add(Float.valueOf(df.format(average(userId2))));
        workScore.add(Float.valueOf(df.format(average(userId3))));
        workScore.add(Float.valueOf(df.format(average(userId4))));
        workScore.add(Float.valueOf(df.format(average(userId5))));
        workScore.add(Float.valueOf(df.format(average(userId6))));
        workScore.add(Float.valueOf(df.format(average(userId7))));
        workScore.add(Float.valueOf(df.format(average(userId8))));
        workScore.add(Float.valueOf(df.format(average(userId9))));
        workScore.add(Float.valueOf(df.format(average(userId10))));
        workScore.add(Float.valueOf(df.format(average(userId11))));
        workScore.add(Float.valueOf(df.format(average(userId12))));
        workScore.add(Float.valueOf(df.format(average(userId13))));
        workScore.add(Float.valueOf(df.format(average(userId14))));
        workScore.add(Float.valueOf(df.format(average(userId15))));
        workScore.add(Float.valueOf(df.format(average(userId16))));
        workScore.add(Float.valueOf(df.format(average(userId17))));
        workScore.add(Float.valueOf(df.format(average(userId18))));
        workScore.add(Float.valueOf(df.format(average(userId19))));
        workScore.add(Float.valueOf(df.format(average(userId20))));

        Result3 result3=new Result3();
        result3.setWork(work);
        result3.setWorkScore(workScore);

        long endTime = System.currentTimeMillis();    //获取结束时间

        System.out.println("程序运行时间3：" + (endTime - startTime) + "ms");    //输出程序运行时间
        return result3;

    }



    public float average(List<Integer> userIds){
        int number=0;
        float sum=0;
        for (Integer integer : userIds) {
            List<Double> rating=userRatingMap.get(integer);
            number+=rating.size();
            for (Double aFloat : rating) {
                sum+=aFloat;
            }

        }

        return sum/number;
    }


}
