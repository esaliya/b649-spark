package edu.indiana.cs.b649.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class SparkWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName
                ("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("file://" +
                "./src/main/resources/wc/words.txt");
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
        counts.saveAsTextFile("file://./src/main/resources/wc/words.count.txt");
    }
}
