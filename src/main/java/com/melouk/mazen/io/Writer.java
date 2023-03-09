package com.melouk.mazen.io;

import org.apache.spark.api.java.JavaPairRDD;

public class Writer {

    public void write(JavaPairRDD<String, Long> processed,String outputPath) {
        processed.saveAsTextFile(outputPath);
    }
}
