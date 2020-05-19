package alluxio.stress.worker;

import alluxio.stress.BaseParameters;
import alluxio.stress.JsonSerializable;
import alluxio.stress.TaskResult;
import alluxio.stress.job.IOConfig;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.stress.master.MasterBenchSummary;
import alluxio.stress.master.MasterBenchTaskResult;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@NotThreadSafe
public class IOTaskResult implements TaskResult {
    private List<Point> mPoints;
    private List<String> mErrors;
    private BaseParameters mBaseParameters;
    private WorkerBenchParameters mParameters;

    public IOTaskResult() {
        mPoints = new ArrayList<>();
        mErrors = new ArrayList<>();
    }

    @JsonCreator
    public IOTaskResult(@JsonProperty("points") List<Point> points,
                        @JsonProperty("errors") List<String> errors) {
        mPoints = points;
        mErrors = errors;
    }

    public void addPoint(Point p) {
        mPoints.add(p);
    }

    public List<Point> getPoints() {
        return mPoints;
    }

    public void addError(String errorMsg) {
        mErrors.add(errorMsg);
    }

    public List<String> getErrors() {
        return mErrors;
    }

    public void setErrors(List<String> errors) {
        mErrors = errors;
    }

    public void setPoints(List<Point> points) {
        mPoints = points;
    }

    public BaseParameters getBaseParameters() {
        return mBaseParameters;
    }

    public void setBaseParameters(BaseParameters baseParameters) {
        this.mBaseParameters = baseParameters;
    }

    public WorkerBenchParameters getParameters() {
        return mParameters;
    }

    public void setParameters(WorkerBenchParameters parameters) {
        this.mParameters = parameters;
    }

    public IOTaskResult merge(IOTaskResult anotherResult) {
        mPoints.addAll(anotherResult.getPoints());
        mErrors.addAll(anotherResult.getErrors());
        mBaseParameters = anotherResult.getBaseParameters();
        mParameters = anotherResult.getParameters();
        return this;
    }
    public static IOTaskResult reduceList(Iterable<IOTaskResult> results) {
        IOTaskResult aggreResult = new IOTaskResult();
        for (IOTaskResult r : results) {
            aggreResult.merge(r);
        }
        return aggreResult;
    }

    @Override
    public String toString() {
        return String.format("Points=%s, Errors=%s",
                mPoints, mErrors);
    }

    public static class Point implements JsonSerializable {
        // TODO(jiacheng): getter and setter
        public IOConfig.IOMode mMode;
        public double mDurationMs;
        public int mDataSizeMB;

        @JsonCreator
        public Point(@JsonProperty("mode") IOConfig.IOMode mode,
                     @JsonProperty("durationMs") double duration,
                     @JsonProperty("dataSizeMB") int dataSize) {
            mMode = mode;
            mDurationMs = duration;
            mDataSizeMB = dataSize;
        }

        @Override
        public String toString() {
            return String.format("{mode=%s, duration=%s, dataSize=%s}",
                    mMode, mDurationMs, mDataSizeMB);
        }

        @Override
        public boolean equals(Object other) {
            if (! (other instanceof Point)) {
                return false;
            }
            Point b = (Point) other;
            return this.mMode == b.mMode && this.mDataSizeMB == b.mDataSizeMB
                    && this.mDurationMs == b.mDurationMs;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(mMode, mDataSizeMB, mDurationMs);
        }
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
