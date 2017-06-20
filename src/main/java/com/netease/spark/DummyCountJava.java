package com.netease.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DummyCountJava {
    private static List<Integer> makeList(int from, int to) {
        assert(from <= to);
        ArrayList<Integer> result = new ArrayList<>();
        while (from < to) {
            result.add(from++);
        }
        return result;
    }

    public static void main(String [] args) {
        SparkConf conf = new SparkConf().setAppName("DummyCountJava");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> distData = sc.parallelize(makeList(1, 101), 100);

        // Avaliable in Spark-1.6; Departed in Spark-2.x;
        /**
        long result = distData.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterable<Integer> call(Integer integer) throws Exception {
                return makeList(1, 101);
            }
        }).count();*/

        // Spark-2.x;
        long result = distData.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterator<Integer> call(Integer integer) {
                return makeList(1, 101).iterator();
            }
        }).count();

        System.out.println(String.format("count(100 * 100) is %d", result));
    }
}
