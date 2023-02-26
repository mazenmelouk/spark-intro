package com.melouk.mazen;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class Reader {
    private final SparkSession sparkSession;

    public Reader(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

   public JavaRDD<String> readTextFile(String path) {
        return sparkSession.sparkContext().textFile(path, 5).toJavaRDD();
    }
}
