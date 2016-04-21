package edu.indiana.cs.b649.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.stream.IntStream;

public class SparkKMeans {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Primer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int n = Integer.parseInt(args[0]);
        int k = Integer.parseInt(args[1]);
        int iterations = Integer.parseInt(args[2]);

        double threshold = 0.001;
        int d = 3;

        double[][] initialCenters = new double[k][d];
        IntStream.range(0, k).forEach(i -> IntStream.range(0,d).forEach(j->
                initialCenters[i][j] = Math.random()));

        for (int i = 0; i < iterations; ++i) {
            Broadcast<double[][]> centers = sc.broadcast(initialCenters);

        }

    }
}
