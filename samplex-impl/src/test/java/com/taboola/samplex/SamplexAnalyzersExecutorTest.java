package com.taboola.samplex;


import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableSet;
import com.taboola.schemafilter.SchemaFilter;
import com.taboola.schemafilter.TopLevelFieldsSchemaWhitelistFilter;

public class SamplexAnalyzersExecutorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSamplex() throws IOException {
        SparkSession sparkSession = SparkForTest.sparkSession;
        final Dataset<Row> nobelPrizeWinners = SparkForTest.getTestDataInput();
        SamplexExecutor samplexExecutor = new SamplexExecutor(sparkSession);

        // Local file system to save in, may be local aka file:/// or HDFS aka hdfs://your_name_node
        final FileSystem fileSystem = FileSystem.get(new Configuration());

        final String bornInRussiaDestPath = temporaryFolder.newFolder("born-in-russia").getAbsolutePath();
        SamplexJob filterBornInRussiaCountry = new SamplexJob() {
            @Override
            public SamplexFilter getRecordFilter() {
                return (record) -> "Russia".equals(record.get("bornCountry"));
            }

            @Override
            public String getDestinationFolder() {
                return bornInRussiaDestPath;
            }
        };

        final String diedInGBDestPath = temporaryFolder.newFolder("died-in-gb").getAbsolutePath();
        SamplexJob filterDiedInGB = new SamplexJob() {
            @Override
            public SamplexFilter getRecordFilter() {
                return (record) -> "GB".equals(record.get("diedCountryCode"));
            }

            @Override
            public String getDestinationFolder() {
                return diedInGBDestPath;
            }

            @Override
            public SchemaFilter getSchemaFilter() {
                return new TopLevelFieldsSchemaWhitelistFilter(ImmutableSet.of("diedCity"));
            }
        };

        samplexExecutor.executeSamplex(
                        fileSystem,
                        nobelPrizeWinners,
                        Arrays.asList(filterBornInRussiaCountry, filterDiedInGB),
                        1);


        // now we will verify samplex outputs with regular Spark and compare to regular spark operation

        // First "filterBornInRussiaCountry"
        Dataset <Row> samplexOutputBornInRussia = sparkSession.read().parquet(bornInRussiaDestPath);
        Dataset <Row> expectedBornInRussia = nobelPrizeWinners.filter("bornCountry == 'Russia'");
        assertEquals(expectedBornInRussia.schema(), samplexOutputBornInRussia.schema());
        assertEquals(expectedBornInRussia.count(), samplexOutputBornInRussia.count());

        // First "filterDiedInGB"
        Dataset <Row> samplexOutputDiedInGB = sparkSession.read().parquet(diedInGBDestPath);
        Dataset <Row> expectedDiedInGB = nobelPrizeWinners
                .filter("diedCountryCode == 'GB'")
                .select("diedCity");

        assertEquals(expectedDiedInGB.schema(), samplexOutputDiedInGB.schema());
        assertEquals(expectedDiedInGB.count(), samplexOutputDiedInGB.count());
    }
}