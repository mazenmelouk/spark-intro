package com.melouk.mazen.music;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_json;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static SparkSession createSession() {
        return SparkSession
                .builder()
                .appName("SparkMusicApplication")
                .getOrCreate();
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Must provide input paths for tracks, artist and output paths");
        }
        logger.info("Running on cluster");

        String tracksPath = args[0];
        String artistsPath = args[1];
        String outputPath = args[2];

        try (SparkSession session = createSession()) {
            Dataset<Row> artists = session.read().option("header", true).csv(artistsPath);
            Dataset<Row> tracks = session.read().option("header", true).csv(tracksPath);
            tracks = tracks.withColumn(
                    "track_id_artists",
                    from_json(tracks.col("track_id_artists"), DataTypes.createArrayType(DataTypes.StringType))
            );
            tracks = tracks.withColumn("track_artist_id", explode(tracks.col("track_id_artists")));


            Dataset<Row> join = tracks.join(artists, tracks.col("track_artist_id").equalTo(artists.col("artist_id")));

            join.write().json(outputPath);
        }


    }
}
