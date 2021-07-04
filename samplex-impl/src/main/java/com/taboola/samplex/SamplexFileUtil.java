package com.taboola.samplex;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import avro.shaded.org.apache.parquet.hadoop.metadata.CompressionCodecName;
import io.vavr.CheckedConsumer;
import io.vavr.CheckedFunction1;

class SamplexFileUtil {

    private final static String ATTEMPT_REGEX_STRING = "^.*part-\\d+-(\\d+).[a-z0-9]+.parquet$";

    private static String getFilePrefixWithoutAttemptNumber(String fileName) {
        return fileName.substring(0, fileName.lastIndexOf("-"));
    }

    private static List<String> getFilesToBeRemoved(List<String> duplicateFiles) {
        Comparator<ImmutablePair<Integer, String>> comparator = Comparator.comparingInt(ImmutablePair::getLeft);

        return duplicateFiles.stream()
                .map(fileName -> new ImmutablePair<>(extractAttemptIdFromFileName(fileName), fileName))
                .sorted(comparator.reversed())
                .skip(1) // Remove last (max) attemptID
                .map(ImmutablePair::getRight)
                .collect(Collectors.toList());
    }


    private static int extractAttemptIdFromFileName(String fileName) {
        Matcher matcher = Pattern.compile(ATTEMPT_REGEX_STRING).matcher(fileName);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            throw new RuntimeException("Failed to get attempt id from file path");
        }
    }


    static void removeFailedTasksFiles(List<SamplexJobSpecificContext> jobSpecificContexts, FileSystem fs) {
        CheckedFunction1<Path, FileStatus[]> checkedListFiles = fs::listStatus;
        CheckedConsumer<Path> checkedDelete = path -> fs.delete(path, true);

        jobSpecificContexts.stream()
                        .map(context -> new Path(context.getOutputPath())) // Get all output paths
                .map(checkedListFiles.unchecked())
                .map(Arrays::asList) // Get all files from output folders
                .map(listFileStatus -> listFileStatus.stream().map(fileStatus -> fileStatus.getPath().toString()).collect(Collectors.toList()))
                .flatMap(List::stream) // flat all output files statuses
                .collect(groupingBy(SamplexFileUtil::getFilePrefixWithoutAttemptNumber, toList())) // group by file prefix without attempt
                .values().stream().map(SamplexFileUtil::getFilesToBeRemoved)
                .flatMap(List::stream)
                .map(Path::new)
                .forEach(checkedDelete.unchecked());
    }

    static void removeOutputFolders(List<SamplexJobSpecificContext> jobSpecificContexts, FileSystem fileSystem) throws IOException {
        CheckedConsumer<String> checkedDelete = (pathString) -> {
            Path path = new Path(pathString);
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
        };
        jobSpecificContexts.stream()
                .map(SamplexJobSpecificContext::getOutputPath)
                .forEach(checkedDelete.unchecked());
    }

    static void writeSuccessFiles(List<SamplexJobSpecificContext> jobSpecificContexts, FileSystem fs) {
        CheckedConsumer<Path> checkedCreateFunction = path -> {
            try (final FSDataOutputStream fsDataOutputStream = fs.create(path)){}
        };
        jobSpecificContexts.stream()
                .map(i -> new Path((StringUtils.appendIfMissing(i.getOutputPath(),"/") + "_SUCCESS")))
                .forEach(checkedCreateFunction.unchecked());
    }

    static String createTaskFileName(int taskId, int attemptNum, CompressionCodecName codecName) {
        return String.format("part-%05d-%02d.%s.parquet", taskId, attemptNum, codecName.name().toLowerCase());
    }
}
