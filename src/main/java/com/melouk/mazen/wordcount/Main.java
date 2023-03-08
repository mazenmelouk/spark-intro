package com.melouk.mazen.wordcount;

import com.melouk.mazen.Reader;
import com.melouk.mazen.Writer;
import com.melouk.mazen.utils.LocalFileSystemUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkIntro/WordCount");
        String inputPath, outputPath;
        if (args.length != 2) {
            logger.info("Running locally");
            inputPath = LocalFileSystemUtils.getPathForResource("word-count-example.txt");
            outputPath = "/tmp/word-count-output/" + LocalDateTime.now();
            sparkConf.setMaster("local");
        } else {
            logger.info("Running on cluster");

            inputPath = args[0];
            outputPath = args[1];
        }

        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            Reader reader = new Reader(new SparkSession(jsc.sc()));
            Writer writer = new Writer();
            SimpleWordCount simpleWordCount = new SimpleWordCount();
            WordCountService wordCountService = new WordCountService(reader, simpleWordCount, writer);
            wordCountService.performWordCount(inputPath, outputPath);
        }

    }
}
