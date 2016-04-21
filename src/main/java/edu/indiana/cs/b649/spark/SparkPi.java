package edu.indiana.cs.b649.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

public class SparkPi {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Pi");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // The number of samples we like to check
        int NUM_SAMPLES = 100;

        // Create a list of numbers from 0 to NUM_SAMPLES - 1
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        // Make an RDD from the numbers and execute filter transformation in
        // parallel followed by a count action.
        // Note. we actually ignore the number value (i). Usually, this
        // pattern is called iterating over an index set in parallel.
        long count = sc.parallelize(l).filter((Function<Integer, Boolean>) i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1;
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }
}
