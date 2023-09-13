package alluxio.stress.worker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.List;

@JsonDeserialize(using = WorkerBenchCoarseDataPointDeserializer.class)
public class WorkerBenchCoarseDataPoint {
    // properties: workerId, threadId, sliceId, records
    @JsonProperty("wid")
    private Long mWid;
    @JsonProperty("tid")
    private Long mTid;
    @JsonProperty("data")
    private List<List<WorkerBenchDataPoint>> mData;
    @JsonProperty("throughput")
    private List<Long> mThroughput;

    // constructor
    public WorkerBenchCoarseDataPoint(Long workerID, Long threadID, List<List<WorkerBenchDataPoint>> data,
                                      List<Long> throughput) {
        mWid = workerID;
        mTid = threadID;
        mData = data;
        mThroughput = throughput;
    }

    // getter & setters
    public Long getWid() {
        return mWid;
    }

    public void setWid(Long wid) {
        mWid = wid;
    }

    public Long getTid() {
        return mTid;
    }

    public void setTid(Long tid) {
        mTid = tid;
    }

    public List<List<WorkerBenchDataPoint>> getData() {
        return mData;
    }

    public void setData(List<List<WorkerBenchDataPoint>> data) {
        mData = data;
    }

    public void addDataPoints(List<WorkerBenchDataPoint> data) {
        mData.add(data);
    }

    public List<Long> getThroughput() {
        return mThroughput;
    }

    public void setThroughput(List<Long> throughput) {
        mThroughput = throughput;
    }

    public void clearThroughput() {
        mThroughput.clear();
    }
}
