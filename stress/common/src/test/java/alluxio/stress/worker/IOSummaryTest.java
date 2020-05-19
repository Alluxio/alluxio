package alluxio.stress.worker;

import alluxio.stress.BaseParameters;
import alluxio.stress.job.IOConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IOSummaryTest {
    @Test
    public void json() throws Exception {
        IOTaskResult result = new IOTaskResult();
        result.addPoint(new IOTaskResult.Point(IOConfig.IOMode.READ, 100L, 20));
        result.addPoint(new IOTaskResult.Point(IOConfig.IOMode.WRITE, 100L, 5));
        IOTaskSummary summary = new IOTaskSummary(result);

        // params
        BaseParameters baseParams = new BaseParameters();
        baseParams.mCluster = true;
        baseParams.mDistributed = true;
        baseParams.mId = "mock-id";
        baseParams.mStartMs = 0L;
        baseParams.mInProcess = false;
        summary.setBaseParameters(baseParams);

        WorkerBenchParameters workerParams = new WorkerBenchParameters();
        workerParams.mPath = "hdfs://path";
        summary.setParameters(workerParams);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(summary);
        System.out.println("Object mapper converted: " +json);
        System.out.println(summary.toJson());

        IOTaskSummary other = mapper.readValue(json, IOTaskSummary.class);
        System.out.println(other.toJson());
    }

    @Test
    public void statJson() throws Exception {
        IOTaskResult result = new IOTaskResult();
        result.addPoint(new IOTaskResult.Point(IOConfig.IOMode.READ, 100L, 20));
        result.addPoint(new IOTaskResult.Point(IOConfig.IOMode.WRITE, 100L, 5));
        IOTaskSummary summary = new IOTaskSummary(result);
        IOTaskSummary.SpeedStat stat= summary.getReadSpeedStat();
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(stat);
        System.out.println("Object mapper converted: " +json);

        IOTaskSummary.SpeedStat other = mapper.readValue(json, IOTaskSummary.SpeedStat.class);
        System.out.println("Parsed back from json");
        System.out.println(other);
    }

    public void checkEquality(IOTaskSummary a, IOTaskSummary b) {
        assertEquals(a.getPoints().size(), b.getPoints().size());
        Set<IOTaskResult.Point> points = new HashSet<>(a.getPoints());
        for (IOTaskResult.Point p : b.getPoints()) {
            assertTrue(points.contains(p));
        }
        assertEquals(a.getErrors().size(), b.getErrors().size());
        Set<String> errors = new HashSet<>(a.getErrors());
        for (String e : b.getErrors()) {
            assertTrue(errors.contains(e));
        }
        return;
    }
}
