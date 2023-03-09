package com.melouk.mazen.countries;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {//Runs locally only
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Country")
                .master("local")
                .getOrCreate();
        String languagesPath = Main.class.getClassLoader().getResource("country-by-languages.json").getPath();
        String populationPath = Main.class.getClassLoader().getResource("country-by-population.json").getPath();
        Dataset<Row> languages = spark.read().option("multiline",true).json(languagesPath);
        Dataset<Row> populations = spark.read().option("multiline",true).json(populationPath);
        Dataset<Row> countLanguages = languages.selectExpr("country", "size(languages) as count");
        countLanguages.join(populations,"country").sample(0.2).show();
     //   Thread.sleep(Duration.ofHours(1).toMillis()); Uncomment for local access of SparkUI
    }
}
