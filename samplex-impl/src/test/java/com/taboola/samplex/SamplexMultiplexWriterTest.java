package samplex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.databricks.spark.avro.SchemaConverters;
import com.google.common.collect.ImmutableSet;
import com.taboola.schemafilter.RecursiveIteratingSchemaWhitelistFilter;
import com.taboola.schemafilter.TopLevelFieldsSchemaBlacklistFilter;


public class SamplexMultiplexWriterTest {

    private final SamplexMultiplexWriter samplexMultiplexWriter = new SamplexMultiplexWriter();
    private final SamplexFilter bornInRussiaFilter = (record) -> "Russia".equals(record.get("bornCountry"));

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCallNoSchemaFilter() throws Exception {

        SparkSession sparkSession = SparkForTest.sparkSession;
        final Dataset<Row> nobelPrizeDf = SparkForTest.getTestDataInput();
        final long sparkSqlExpected = nobelPrizeDf.filter("bornCountry == 'Russia'").count();

        // Create Avro schema, used to read from
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("spark_schema").namespace(null);
        Schema dfAvroSchema = SchemaConverters.convertStructToAvro(nobelPrizeDf.schema(), recordBuilder, null);

        String samplexOutputFolder = temporaryFolder.newFolder("output").getPath();
        SamplexJobSpecificContext specificContext = new SamplexJobSpecificContext(samplexOutputFolder, "BornInRussia", bornInRussiaFilter, null);
        SamplexContext samplexContext = new SamplexContext(nobelPrizeDf.inputFiles()[0], Collections.singletonList(specificContext), dfAvroSchema.toString());
        samplexMultiplexWriter.call(Collections.singletonList(samplexContext).iterator());

        long countAfterSamplex = sparkSession.read().parquet(samplexOutputFolder).count();
        assertEquals(sparkSqlExpected, countAfterSamplex);
    }

    @Test
    public void testCallBlacklistSchemaFilter() throws Exception {

        SparkSession sparkSession = SparkForTest.sparkSession;
        final Dataset<Row> nobelPrizeDf = SparkForTest.getTestDataInput();
        final long sparkSqlExpected = nobelPrizeDf.filter("bornCountry == 'Russia'").count();

        String samplexOutputFolder = temporaryFolder.newFolder("output").getPath();

        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("spark_schema").namespace(null);
        Schema dfAvroSchema = SchemaConverters.convertStructToAvro(nobelPrizeDf.schema(), recordBuilder, null);

        final String genderColumnToFilter = "gender";
        Set<String> blacklistFields = ImmutableSet.of(genderColumnToFilter);

        SamplexJobSpecificContext specificContext = new SamplexJobSpecificContext(
                samplexOutputFolder, "BornInRussia",
                bornInRussiaFilter, new TopLevelFieldsSchemaBlacklistFilter(blacklistFields));

        SamplexContext samplexContext = new SamplexContext(nobelPrizeDf.inputFiles()[0], Collections.singletonList(specificContext), dfAvroSchema.toString());
        samplexMultiplexWriter.call(Collections.singletonList(samplexContext).iterator());

        Dataset<Row> result = sparkSession.read().parquet(samplexOutputFolder);
        long countAfterSamplex = result.count();
        assertEquals(sparkSqlExpected, countAfterSamplex);

        // Need to verify that we filtered a gender column
        assertFalse(Arrays.asList(result.columns()).contains(genderColumnToFilter));
    }

    @Test
    public void testCallWhitelistSchemaFilter() throws Exception {

        SparkSession sparkSession = SparkForTest.sparkSession;
        final Dataset<Row> nobelPrizeDf = SparkForTest.getTestDataInput();
        final long sparkSqlExpected = nobelPrizeDf.filter("bornCountry == 'Russia'").count();

        String samplexOutputFolder = temporaryFolder.newFolder("output").getPath();

        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("spark_schema").namespace(null);
        Schema dfAvroSchema = SchemaConverters.convertStructToAvro(nobelPrizeDf.schema(), recordBuilder, null);

        // We will check whitelist filter with recursive
        final String onlyColumnToKeep = "prizes.category";
        Set<String> whitelistFields = ImmutableSet.of(onlyColumnToKeep);

        SamplexJobSpecificContext specificContext = new SamplexJobSpecificContext(
                samplexOutputFolder, "BornInRussia",
                bornInRussiaFilter, new RecursiveIteratingSchemaWhitelistFilter(whitelistFields));

        SamplexContext samplexContext = new SamplexContext(nobelPrizeDf.inputFiles()[0], Collections.singletonList(specificContext), dfAvroSchema.toString());
        samplexMultiplexWriter.call(Collections.singletonList(samplexContext).iterator());

        Dataset<Row> resultDataFrame = sparkSession.read().parquet(samplexOutputFolder);
        long countAfterSamplex = resultDataFrame.count();
        assertEquals(sparkSqlExpected, countAfterSamplex);

        // Verify we have only 1 column and it is one from white list
        assertEquals(1, resultDataFrame.columns().length);
        assertEquals("category", resultDataFrame.select("prizes.category").columns()[0]);
    }

    @Test
    public void testGetFullOutputPath() {
        SamplexJobSpecificContext specificContext = new SamplexJobSpecificContext("/output/path/", "analyzer", null, null);
        String fullOutputPath = samplexMultiplexWriter.getFullOutputPath("file.name", specificContext);

        String expectedOutputPath = "/output/path/file.name";
        assertEquals(expectedOutputPath, fullOutputPath);
        fullOutputPath = samplexMultiplexWriter.getFullOutputPath("file.name", specificContext);
        assertEquals(expectedOutputPath, fullOutputPath);
    }
}