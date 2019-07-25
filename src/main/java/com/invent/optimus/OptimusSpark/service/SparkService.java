package com.invent.optimus.OptimusSpark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class SparkService {

    private static final String LOOKUP_FILES_PATH = "adobe/lookup_data/";

    private SparkSession sparkSession;

    public SparkService(SparkSession aSparkSession) {
        sparkSession = aSparkSession;
    }

    public void process() throws IOException {

        List<String> lookupFiles = retrieveLookupFiles();

        constructDataFrames(lookupFiles);

        List<String> headers = retrieveHeaders();

        Dataset hitData = sparkSession.read()
                .option("delimiter", "\t")
                .option("header", false)
                .option("nullValue", "none")
                .schema(constructSchema(headers))
                .csv("adobe/hit_data.tsv");
        hitData.createOrReplaceTempView("hitData");

        //compute and save result
        sparkSession.sql("select sc.*, " + "browser.browser as browser_name, " + "browser_type, " + "connection_type.connection_type as connection_name, " +         "country.country as country_name, " +         "javascript_version, " +         "languages.languages as languages, " +         "operating_systems, " +         "referrer_type, " +         "resolution.resolution as screen_resolution, " +         "search_engines " +         "from hitData as sc " +         "left join browser on sc.browser = browser.id " +         "left join browser_type on sc.browser = browser_type.id " +         "left join connection_type on sc.connection_type = connection_type.id " +         "left join country on sc.country = country.id " +         "left join javascript_version on sc.javascript = javascript_version.id " +         "left join languages on sc.language = languages.id " +         "left join operating_systems on sc.os = operating_systems.id " +         "left join referrer_type on sc.ref_type = referrer_type.id " +         "left join resolution on sc.resolution = resolution.id " +         "left join search_engines on sc.post_search_engine = search_engines.id ")
                .write().mode("overwrite").json("final.json");
    }

    private StructType constructSchema(final List<String> hitDataHeaders) {
        StructType schema = new StructType();

        for(String singleColumn : hitDataHeaders) {
            schema = schema.add(singleColumn, DataTypes.StringType, true);
        }

        return schema;
    }

    private void constructDataFrames(final List<String> lookUpFiles) {
        lookUpFiles.forEach(lookUp -> {
            String columnName = lookUp.replace(".tsv", "");
            Dataset lookupPart = sparkSession.read()
                    .option("delimiter", "\t")
                    .schema(constructLookupSchema(columnName))
                    .csv(LOOKUP_FILES_PATH + lookUp);
            lookupPart.createOrReplaceTempView(columnName);
        });
    }

    private StructType constructLookupSchema(String columnName) {
        return new StructType()
                .add("id", DataTypes.StringType, true)
                .add(columnName, DataTypes.StringType, true);
    }

    private List<String> retrieveLookupFiles() {
        return Arrays
                .stream(Objects.requireNonNull(new File("adobe/lookup_data")
                        .listFiles()))
                .map(File::getName)
                .filter(f -> !f.equals("column_headers.tsv"))
                .collect(Collectors.toList());
    }

    private List<String> retrieveHeaders() throws IOException {

        return new ArrayList<>(Arrays.asList(Files
                .readAllLines(Paths
                        .get("adobe/lookup_data/column_headers.tsv"))
                .get(0)
                .split("\\t")));
    }

}
