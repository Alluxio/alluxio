package alluxio.stress.worker;

import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;

import java.util.ArrayList;
import java.util.List;

public class IOTaskSummary implements Summary {
    private List<IOTaskResult.Point> mPoints;
    private List<String> mErrors;

    public IOTaskSummary(IOTaskResult result) {
        mPoints = new ArrayList<>(result.getPoints());
        mErrors = new ArrayList<>(result.getErrors());
    }

    @Override
    public GraphGenerator graphGenerator() {
        // TODO(jiacheng): what is a graph???
        return null;
    }

    // TODO(jiacheng): standard deviation?

    @Override
    public String toString() {
        return String.format("IOTaskSummary: {Points={}, Errors={}}",
                mPoints, mErrors);
    }
}
