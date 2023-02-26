package com.melouk.mazen.wordcount;

import com.melouk.mazen.Reader;
import com.melouk.mazen.Writer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class WordCountService {
    private final Reader reader;
    private final SimpleWordCount simpleWordCount;
    private final Writer writer;

    public WordCountService(Reader reader, SimpleWordCount simpleWordCount, Writer writer) {
        this.reader = reader;
        this.simpleWordCount = simpleWordCount;
        this.writer = writer;
    }

    public void performWordCount(String inputPath,
                                  String outputPath) {
        JavaRDD<String> input = reader.readTextFile(inputPath);
        JavaPairRDD<String, Long> processed = simpleWordCount.count(input);
        writer.write(processed, outputPath);
    }
}
