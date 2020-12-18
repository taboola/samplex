package samplex;

import java.util.concurrent.BlockingQueue;

import lombok.Value;

@Value
public class SamplexWriterData {

    String jobId;
    String recordSchema;
    String outputFile;
    boolean useFieldNameModel;
    SamplexFilter samplexFilter;
    BlockingQueue<SamplexRecord> blockingQueue;

    SamplexWriterData(
            SamplexFilter samplexFilter,
            String jobId,
            BlockingQueue<SamplexRecord> blockingQueue,
            String recordSchema,
            String outputFile, boolean useFieldNameModel) {
        this.samplexFilter = samplexFilter;
        this.jobId = jobId;
        this.blockingQueue = blockingQueue;
        this.recordSchema = recordSchema;
        this.outputFile = outputFile;
        this.useFieldNameModel = useFieldNameModel;
    }
}
