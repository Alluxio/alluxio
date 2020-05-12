package alluxio.stress.job;

import alluxio.job.plan.PlanConfig;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.util.FormatUtils;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class IOConfig extends StressBenchConfig {
    private static final String NAME = "IO";

    // READ or WRITE mode
    private WorkerBenchParameters.IOMode mMode;

    // How many streams to write to HDFS concurrently
    private int mThreadNum;

    // Size of data to write in total
    // They will be read in the read performance test
    private long mDataSize;

    // Temp dir to generate test files in
    private String mUfsTempDirPath;

    private int mWorkerNum;

    public IOConfig(@JsonProperty("className") String className,
                    @JsonProperty("args") List<String> args,
                    @JsonProperty("startDelayMs") long startDelayMs,
                    // TODO(jiacheng): how to make this json?
                    WorkerBenchParameters params) {
        super(className, args, startDelayMs);
        mMode = params.mMode;
        mThreadNum = params.mThreads;
        mDataSize = FormatUtils.parseSpaceSize(params.mDataSize);
        mUfsTempDirPath = params.mUfsTempDirPath;
        mWorkerNum = params.mWorkerNum;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public String getUfsTempDirPath() {
        return mUfsTempDirPath;
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
}
