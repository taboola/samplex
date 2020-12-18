package samplex;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkForTest {
    public static SparkSession sparkSession;

    static {
        SparkConf conf = new SparkConf();
        conf.set("spark.default.parallelism", "1");
        conf.set("spark.sql.shuffle.partitions", "1");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.broadcast.compress", "false");
        conf.set("spark.shuffle.compress", "false");
        conf.set("spark.shuffle.spill.compress", "false");
        SparkContext sc = new SparkContext("local", "samplex-test", conf);
        sparkSession = new SparkSession(sc);
    }

    public static Dataset<Row> getTestDataInput() {
        SparkSession sparkSession = SparkForTest.sparkSession;
        String path = "src/test/resources/nobel_prize.snappy.parquet";
        File file = new File(path);
        return sparkSession.read().parquet(file.getAbsolutePath());
    }
}
