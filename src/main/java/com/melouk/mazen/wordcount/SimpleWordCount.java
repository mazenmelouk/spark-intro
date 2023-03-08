package com.melouk.mazen.wordcount;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


import static java.util.Arrays.stream;

public class SimpleWordCount {
    private static final Logger logger = LoggerFactory.getLogger(SimpleWordCount.class);

    JavaPairRDD<String, Long> count(JavaRDD<String> lines) {
        JavaRDD<String> words = lines.map(line -> line.split("\\s")).flatMap(tokens -> stream(tokens).iterator());
        JavaRDD<String> cleaned = words
                .map(SimpleWordCount::clean)
                .filter(word -> !word.isEmpty());
        logger.info("Clean up word, will start word counting!");
        return cleaned.mapToPair(word -> new Tuple2<>(word, 1L)).reduceByKey(Long::sum).sortByKey();
    }

    private static String clean(String word) {
        logger.info("cleaning {}", word);
        return word.replaceAll("[,.!?:;]", "").toLowerCase();
    }
}
