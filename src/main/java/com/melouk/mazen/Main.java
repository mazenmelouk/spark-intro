package com.melouk.mazen;

import com.melouk.mazen.wordcount.SimpleWordCount;
import com.melouk.mazen.wordcount.WordCountService;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.internal.config.R;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark-Intro")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        Reader reader = new Reader(new SparkSession(jsc.sc()));
        Writer writer = new Writer();
        SimpleWordCount simpleWordCount = new SimpleWordCount();
        WordCountService wordCountService = new WordCountService(reader, simpleWordCount, writer);
        wordCountService.performWordCount(
                Main.class.getClassLoader().getResource("word-count-example.txt").getPath(),
                "/tmp/spark-intro/word-count/"
        );
    }
}