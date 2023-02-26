package com.melouk.mazen.wordcount;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

import static java.util.Arrays.stream;

public class SimpleWordCount {

    JavaPairRDD<String, Long> count(JavaRDD<String> lines) {
        JavaRDD<String> words = lines.map(line -> line.split("\\s")).flatMap(tokens -> stream(tokens).iterator());
        JavaRDD<String> cleaned = words
                .map(word -> word.replaceAll("[,.!?:;]", ""))
                .filter(word -> !word.isEmpty())
                .map(String::toLowerCase);

        return cleaned.mapToPair(word -> new Tuple2<>(word, 1L)).reduceByKey(Long::sum).sortByKey();
    }
}
