package alluxio.stress;

import java.util.List;
import java.util.Map;

public abstract class GeneralBenchSummary implements Summary {
    protected float mThroughput;
    protected Map<String, List<String>> mErrors;

    /**
     * @return the throughput
     */
    public float getThroughput() {
        return mThroughput;
    }

    /**
     * @param throughput the throughput
     */
    public void setThroughput(float throughput) {
        mThroughput = throughput;
    }

    /**
     * @return the errors
     */
    public Map<String, List<String>> getErrors() {
        return mErrors;
    }

    /**
     * @param errors the errors
     */
    public void setErrors(Map<String, List<String>> errors) {
        mErrors = errors;
    }
}
