package com.taboola.samplex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.taboola.schemafilter.SchemaFilter;


public class SamplexMultiplexWriter implements VoidFunction<Iterator<SamplexContext>> {

    private final static Logger logger = LogManager.getLogger(SamplexMultiplexWriter.class);
    private final static int MAX_WAIT_THREAD_POOL_SHUTDOWN = Integer.MAX_VALUE;
    private final static int MAX_QUEUE_SIZE = 100000;

    @Override
    public void call(Iterator<SamplexContext> samplexContextIterator) {

        List<SamplexContext> samplexContextList = new ArrayList<>();
        samplexContextIterator.forEachRemaining(samplexContextList::add);

        int taskId = TaskContext.getPartitionId();
        logger.info("Task id [" + taskId + "], Going to work on following files :" + samplexContextList.stream().map(SamplexContext::getInputFile).collect(Collectors.joining(",")));

        boolean failed = false;

        final List<SamplexWriterData> samplexWriterDataList = createSamplexWriterLists(samplexContextList);
        ListeningExecutorService writersExecutorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(samplexWriterDataList.size()));
        ListeningExecutorService readFilterExecutorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
        try {
            List<ListenableFuture<?>> futureWriterList = samplexWriterDataList.stream()
                    .map(samplexWriterData -> writersExecutorService.submit(new AsyncSamplexRecordWriter(samplexWriterData, taskId)))
                    .collect(Collectors.toList());


            List<ListenableFuture<?>> futureReaderList = samplexContextList.stream()
                    .map(samplexContext -> readFilterExecutorService.submit(new AsyncSamplexRecordReader(samplexContext.getInputFile(), samplexWriterDataList, taskId)))
                    .collect(Collectors.toList());


            Futures.allAsList(futureReaderList).get();
            writeTerminationRecord(samplexWriterDataList);
            Futures.allAsList(futureWriterList).get();
        } catch (Throwable t) {
            failed = true;
            String errorMsg = "Failed to read/write batch of files, taskId [" + taskId + "]";
            logger.error(errorMsg, t);
            throw new RuntimeException(errorMsg, t);
        } finally {
            try {
                shutdownExecutorService(failed, readFilterExecutorService);
                shutdownExecutorService(failed, writersExecutorService);
            } catch (Throwable t) {
                logger.error("Failed to close thread pools", t);
            }
        }
    }

    private void writeTerminationRecord(List<SamplexWriterData> samplexWriterDataList) throws InterruptedException {
        for (SamplexWriterData samplexWriterData : samplexWriterDataList) {
            samplexWriterData.getBlockingQueue().put(new SamplexRecord(true, null));
        }
    }

    private void shutdownExecutorService(boolean failed, ExecutorService executorService) {
        if (executorService != null) {
            int timeToWait = failed ? 30 : MAX_WAIT_THREAD_POOL_SHUTDOWN;
            shutDownThreadPool(executorService, timeToWait, TimeUnit.SECONDS);
        }
    }

    private List<SamplexWriterData> createSamplexWriterLists(List<SamplexContext> samplexContexts) {
        SamplexContext samplexContext = samplexContexts.get(0);
        return samplexContext.getSamplexJobSpecificContextList().stream()
                .map(samplexSpecificContext -> {
                    int attemptNum = null == TaskContext.get() ? 0 : TaskContext.get().attemptNumber();
                    String fileName = SamplexFileUtil.createTaskFileName(TaskContext.getPartitionId(), attemptNum, samplexSpecificContext.getCodecName());
                    return createSamplexWriterData(samplexSpecificContext, samplexContext.getAvroSchema(), getFullOutputPath(fileName, samplexSpecificContext));
                })
                .collect(Collectors.toList());
    }

    private SamplexWriterData createSamplexWriterData(SamplexJobSpecificContext samplexSpecificContext, String explodedPageViewSchema, String outputFile) {
        String recordSchema = explodedPageViewSchema;
        boolean useFieldNameModel = false;

        SchemaFilter schemaFilter = samplexSpecificContext.getSchemaFilter();
        if (schemaFilter != null) {
           recordSchema = schemaFilter.filter(explodedPageViewSchema);
           useFieldNameModel = true;
        }

        return SamplexWriterData.builder()
                .samplexFilter(samplexSpecificContext.getSamplexFilter())
                .blockingQueue(new ArrayBlockingQueue<>(MAX_QUEUE_SIZE))
                .outputFile(outputFile)
                .recordSchema(recordSchema)
                .codecName(samplexSpecificContext.getCodecName())
                .jobId(samplexSpecificContext.getJobId())
                .useFieldNameModel(useFieldNameModel)
                .build();
    }

    String getFullOutputPath(String fileName, SamplexJobSpecificContext jobSpecificContext) {
        return StringUtils.appendIfMissing(jobSpecificContext.getOutputPath(), "/") + fileName;
    }


    public static void shutDownThreadPool(ExecutorService pool, long timeOut, TimeUnit timeUnit) {
        try {
            logger.info("shutting down thread pool");
            pool.shutdown();
            if (!pool.awaitTermination(timeOut, timeUnit)) {
                logger.warn("some issue with thread pool shutdown");
                pool.shutdownNow();
                if (!pool.awaitTermination(timeOut, timeUnit)) {
                    logger.error("did not terminate cleanly");
                }
            }
        } catch (InterruptedException e) {
            logger.warn("interrupted during threadpool hutdown");
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("thread pool shutdown");
    }
}
