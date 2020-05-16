package alluxio.stress.worker;

import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;

import java.util.List;

public class IOTaskSummary implements Summary {
    private long mReadDurationMs;
    private long mReadDataSize;
    private List<Exception> mReadErrors;
    private long mWriteDurationMs;
    private long mWriteDataSize;
    private List<Exception> mWriteErrors;

    public IOTaskSummary(IOTaskResult result) {
        mReadDataSize = result.getReadDataSize();
        mReadDurationMs = result.getReadDurationMs();
        mReadErrors = result.getReadErrors();
        mWriteDataSize = result.getWriteDataSize();
        mWriteDurationMs = result.getWriteDurationMs();
        mWriteErrors = result.getWriteErrors();
    }

    @Override
    public GraphGenerator graphGenerator() {
        // TODO(jiacheng): what is a graph???
        return null;
    }

    // TODO(jiacheng): standard deviation?

    @Override
    public String toString() {
        return String.format("IOTaskSummary: {mReadDurationMs=%s,mReadDataSize=%s,mWriteDurationMs=%s,mWriteDataSize=%s}",
                mReadDurationMs, mReadDataSize, mWriteDurationMs, mWriteDataSize);
    }
}
