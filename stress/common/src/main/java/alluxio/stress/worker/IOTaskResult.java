package alluxio.stress.worker;

import alluxio.stress.TaskResult;
import alluxio.stress.master.MasterBenchSummary;
import alluxio.stress.master.MasterBenchTaskResult;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@NotThreadSafe
public class IOTaskResult implements TaskResult {
    private long mReadDurationMs;
    private long mReadDataSize;
    private List<Exception> mReadErrors;
    private long mWriteDurationMs;
    private long mWriteDataSize;
    private List<Exception> mWriteErrors;

    public long getReadDurationMs() {
        return mReadDurationMs;
    }

    public void setReadDurationMs(long mReadDurationMs) {
        this.mReadDurationMs = mReadDurationMs;
    }

    public long getReadDataSize() {
        return mReadDataSize;
    }

    public void setReadDataSize(long mReadDataSize) {
        this.mReadDataSize = mReadDataSize;
    }

    public List<Exception> getReadErrors() {
        return mReadErrors;
    }

    public void addReadError(Exception e) {
        mReadErrors.add(e);
    }

    public long getWriteDurationMs() {
        return mWriteDurationMs;
    }

    public void setWriteDurationMs(long mWriteDurationMs) {
        this.mWriteDurationMs = mWriteDurationMs;
    }

    public long getWriteDataSize() {
        return mWriteDataSize;
    }

    public void setWriteDataSize(long mWriteDataSize) {
        this.mWriteDataSize = mWriteDataSize;
    }

    public List<Exception> getWriteErrors() {
        return mWriteErrors;
    }

    public void addWriteError(Exception e) {
        mWriteErrors.add(e);
    }

    public IOTaskResult() {
        mReadErrors = new ArrayList<>();
        mWriteErrors = new ArrayList<>();
    }


    public void merge(IOTaskResult anotherResult) {
        mReadErrors.addAll(anotherResult.getReadErrors());
        mWriteErrors.addAll(anotherResult.getWriteErrors());
        mReadDataSize += anotherResult.getReadDataSize();
        mWriteDataSize += anotherResult.getWriteDataSize();
        mReadDurationMs += anotherResult.getReadDurationMs();
        mWriteDurationMs += anotherResult.getWriteDurationMs();
    }

    public static IOTaskResult reduceList(Iterable<IOTaskResult> results) {
        IOTaskResult aggreResult = new IOTaskResult();
        for (IOTaskResult r : results) {
            aggreResult.merge(r);
        }
        return aggreResult;
    }

    @Override
    public TaskResult.Aggregator aggregator() {
        return new IOTaskResult.Aggregator();
    }

    private static final class Aggregator implements TaskResult.Aggregator<IOTaskResult> {
        @Override
        public IOTaskSummary aggregate(Iterable<IOTaskResult> results) throws Exception {
            return new IOTaskSummary(reduceList(results));
        }
    }
}
