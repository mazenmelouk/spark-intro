package com.melouk.mazen.wordcount;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.List;

public class SimpleWordCountTest extends SharedJavaSparkContext {

    @Test
    public void testCountsWords() {
        List<String> lines = Arrays.asList(
                "This is an example.",
                "This example is about Spark, using RDDs.",
                "You shouldn't be using RDDs!");

        JavaPairRDD<String, Long> actual = new SimpleWordCount().count(jsc().parallelize(lines));

        JavaPairRDD<String,Long> expected = jsc().parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("about",1L),
                        new Tuple2<>("an",1L),
                        new Tuple2<>("be",1L),
                        new Tuple2<>("example",2L),
                        new Tuple2<>("is",2L),
                        new Tuple2<>("rdds",2L),
                        new Tuple2<>("shouldn't",1L),
                        new Tuple2<>("spark",1L),
                        new Tuple2<>("this",2L),
                        new Tuple2<>("using",2L),
                        new Tuple2<>("you",1L))
        );
        ClassTag<Tuple2<String, Long>> tag =
                scala.reflect.ClassTag$.MODULE$
                        .apply(Tuple2.class);

        JavaRDDComparisons.assertRDDEquals(
                JavaRDD.fromRDD(JavaPairRDD.toRDD(expected),tag),
                JavaRDD.fromRDD(JavaPairRDD.toRDD(actual),tag)
        );
    }
}