/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.indiana.cs.b649.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class SparkWordCount {
    public static void main(String[] args) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        SparkConf conf = new SparkConf().setAppName
                ("WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create RDD from the text file - parallelized over lines.
        JavaRDD<String> textFile = sc.textFile("file://" + args[0] +
                "/src/main/resources/wc/words.txt");

        // Transform textFile RDD into an RDD representing a collection of words
        // That is, each line is split using space, so the parallelization is
        // now over the words.
        JavaRDD<String> words = textFile.flatMap((FlatMapFunction<String,
                String>) s -> Arrays.asList(s.split(" ")));

        // Transform words RDD into a pair RDD. A pair RDD is just a special
        // type of an RDD to represent parallel collections containing keys
        // and values.
        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction
                <String, String, Integer>) s -> new Tuple2<>(s, 1));

        // Perform reduction on the pair RDD. Note. in Spark's terminology
        // reduction is an action where as previous operations are
        // transformations.
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((Function2
                <Integer, Integer, Integer>) (a, b) ->
                a + b);

        counts.saveAsTextFile("file://" + args[0] +
                "/src/main/resources/wc/output/" + dateFormat.format(new Date()));
    }
}
