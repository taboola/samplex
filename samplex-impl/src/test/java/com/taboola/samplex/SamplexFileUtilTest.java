package com.taboola.samplex;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class SamplexFileUtilTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testWriteSuccessFiles() throws IOException {
        Configuration conf = new Configuration();
        String workingDir =  folder.newFolder().getPath();
        Path workDirPath = new Path(workingDir);
        final FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.setWorkingDirectory(workDirPath);

        String outputFolder1 = folder.newFolder("output1").getPath();
        String outputFolder2 = folder.newFolder("output2").getPath();
        SamplexJobSpecificContext specificContext = SamplexJobSpecificContext.builder().outputPath(outputFolder1).jobId("job1").build();
        SamplexJobSpecificContext specificContext2 = SamplexJobSpecificContext.builder().outputPath(outputFolder2).jobId("job2").build();
        SamplexFileUtil.writeSuccessFiles(Arrays.asList(specificContext, specificContext2), fileSystem);

        Assert.assertTrue(fileSystem.exists(new Path(outputFolder1 + "/_SUCCESS")));
        Assert.assertTrue(fileSystem.exists(new Path(outputFolder2 + "/_SUCCESS")));
    }

    @Test
    public void testRemoveOutputFolders() throws IOException {

        Configuration conf = new Configuration();
        String workingDir =  folder.newFolder().getPath();
        Path workDirPath = new Path(workingDir);
        final FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.setWorkingDirectory(workDirPath);

        String outputFolder1 = folder.newFolder("output_remove_1").getPath();
        String outputFolder2 = folder.newFolder("output_remove_2").getPath();
        SamplexJobSpecificContext specificContext = SamplexJobSpecificContext.builder().outputPath(outputFolder1).jobId("job1").build();
        SamplexJobSpecificContext specificContext2 = SamplexJobSpecificContext.builder().outputPath(outputFolder2).jobId("job2").build();
        String fakeOutputPath = "/some/fake/folder";
        SamplexJobSpecificContext specificContext3 = SamplexJobSpecificContext.builder().outputPath(fakeOutputPath).jobId("job2").build();

        Assert.assertTrue(fileSystem.exists(new Path(outputFolder1)));
        Assert.assertTrue(fileSystem.exists(new Path(outputFolder2)));
        Assert.assertFalse(fileSystem.exists(new Path(fakeOutputPath)));

        SamplexFileUtil.removeOutputFolders(Arrays.asList(specificContext, specificContext2, specificContext3), fileSystem);

        Assert.assertFalse(fileSystem.exists(new Path(outputFolder1)));
        Assert.assertFalse(fileSystem.exists(new Path(outputFolder2)));
        Assert.assertFalse(fileSystem.exists(new Path(fakeOutputPath)));
    }

    @Test
    public void testRemoveFailedTasksFiles() throws IOException {
        String outputFolder1 = folder.newFolder("dup_test1").getPath();
        String file1 = folder.newFile("dup_test1/part-00001-00.snappy.parquet").getPath();
        String file2 = folder.newFile("dup_test1/part-00002-00.snappy.parquet").getPath();
        String file3 = folder.newFile("dup_test1/part-00003-00.snappy.parquet").getPath();
        String file3_01 = folder.newFile("dup_test1/part-00003-01.snappy.parquet").getPath();
        String file3_02 = folder.newFile("dup_test1/part-00003-02.snappy.parquet").getPath();
        String file4 = folder.newFile("dup_test1/part-00004-00.snappy.parquet").getPath();
        String file5 = folder.newFile("dup_test1/part-00005-00.snappy.parquet").getPath();
        String file6 = folder.newFile("dup_test1/part-00006-00.snappy.parquet").getPath();
        String file6_01 = folder.newFile("dup_test1/part-00006-01.snappy.parquet").getPath();

        String outputFolder2 = folder.newFolder("dup_test2").getPath();
        String out2_file1 = folder.newFile("dup_test2/part-00001-00.snappy.parquet").getPath();
        String out2_file2 = folder.newFile("dup_test2/part-00002-00.snappy.parquet").getPath();

        SamplexJobSpecificContext specificContext = SamplexJobSpecificContext.builder().outputPath(outputFolder1).jobId("job1").build();

        SamplexJobSpecificContext specificContext2 = SamplexJobSpecificContext.builder().outputPath(outputFolder2).jobId("job2").build();

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);

        Assert.assertTrue(fs.exists(new Path(outputFolder1)));

        Assert.assertTrue(fs.exists(new Path(file1)));
        Assert.assertTrue(fs.exists(new Path(file2)));
        Assert.assertTrue(fs.exists(new Path(file3)));
        Assert.assertTrue(fs.exists(new Path(file3_01)));
        Assert.assertTrue(fs.exists(new Path(file3_02)));
        Assert.assertTrue(fs.exists(new Path(file4)));
        Assert.assertTrue(fs.exists(new Path(file5)));
        Assert.assertTrue(fs.exists(new Path(file6)));
        Assert.assertTrue(fs.exists(new Path(file6_01)));

        Assert.assertTrue(fs.exists(new Path(outputFolder2)));

        Assert.assertTrue(fs.exists(new Path(out2_file1)));
        Assert.assertTrue(fs.exists(new Path(out2_file2)));

        SamplexFileUtil.removeFailedTasksFiles(Arrays.asList(specificContext, specificContext2), fs);

        Assert.assertTrue(fs.exists(new Path(outputFolder1)));

        Assert.assertTrue(fs.exists(new Path(file1)));
        Assert.assertTrue(fs.exists(new Path(file2)));
        Assert.assertFalse(fs.exists(new Path(file3)));
        Assert.assertFalse(fs.exists(new Path(file3_01)));
        Assert.assertTrue(fs.exists(new Path(file3_02)));
        Assert.assertTrue(fs.exists(new Path(file4)));
        Assert.assertTrue(fs.exists(new Path(file5)));
        Assert.assertFalse(fs.exists(new Path(file6)));
        Assert.assertTrue(fs.exists(new Path(file6_01)));

        Assert.assertTrue(fs.exists(new Path(outputFolder2)));

        Assert.assertTrue(fs.exists(new Path(out2_file1)));
        Assert.assertTrue(fs.exists(new Path(out2_file2)));
    }
}