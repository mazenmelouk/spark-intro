package com.melouk.mazen.wordcount;

import com.melouk.mazen.io.Reader;
import com.melouk.mazen.io.Writer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountService {
    private static final Logger logger = LoggerFactory.getLogger(WordCountService.class);
    private final Reader reader;
    private final SimpleWordCount simpleWordCount;
    private final Writer writer;

    public WordCountService(Reader reader, SimpleWordCount simpleWordCount, Writer writer) {
        this.reader = reader;
        this.simpleWordCount = simpleWordCount;
        this.writer = writer;
    }

    public void performWordCount(String inputPath, String outputPath) {
        logger.info("Reading input from {}", inputPath);
        JavaRDD<String> input = reader.readTextFile(inputPath);
        JavaPairRDD<String, Long> processed = simpleWordCount.count(input);
        long count = processed.count();
        logger.info("Writing {} records to {}", count, outputPath);
        writer.write(processed, outputPath);
    }
}
