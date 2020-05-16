package alluxio.stress.job;

import alluxio.job.plan.PlanConfig;
import alluxio.job.plan.replicate.EvictConfig;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.util.FormatUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonTypeName(IOConfig.NAME)
public class IOConfig extends StressBenchConfig {
    public static final String NAME = "IO";

    // READ or WRITE mode
    private WorkerBenchParameters.IOMode mMode;

    // How many streams to write to HDFS concurrently
    private int mThreadNum;

    // Size of data to write in total
    // They will be read in the read performance test
    private int mDataSize;

    // Temp dir to generate test files in
    private String mPath;

    private int mWorkerNum;

    @JsonCreator
    public IOConfig(@JsonProperty("className") String className,
                    @JsonProperty("args") List<String> args,
                    @JsonProperty("startDelayMs") long startDelayMs,
                    @JsonProperty("mode") WorkerBenchParameters.IOMode mode,
                    @JsonProperty("threadNum") int threadNum,
                    @JsonProperty("dataSize") int dataSize,
                    @JsonProperty("workerNum") int workerNum,
                    @JsonProperty("path") String path) {
        super(className, args, startDelayMs);
        mMode = mode;
        mThreadNum = threadNum;
        mDataSize = dataSize;
        mPath = path;
        mWorkerNum = workerNum;
    }

    public IOConfig(@JsonProperty("className") String className,
                    @JsonProperty("args") List<String> args,
                    @JsonProperty("startDelayMs") long startDelayMs,
                    // TODO(jiacheng): how to make this json?
                    WorkerBenchParameters params) {
        super(className, args, startDelayMs);
        mMode = params.mMode;
        mThreadNum = params.mThreads;
        mDataSize = params.mDataSize;
        mPath = params.mPath;
        mWorkerNum = params.mWorkerNum;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public String getPath() {
        return mPath;
    }

    public long getDataSize() {
        return mDataSize;
    }

    public WorkerBenchParameters.IOMode getMode() {
        return mMode;
    }

    public int getThreadNum() {
        return mThreadNum;
    }

    public int getWorkerNum() {
        return mWorkerNum;
    }

    // TODO(jiacheng): toString and equals
}
