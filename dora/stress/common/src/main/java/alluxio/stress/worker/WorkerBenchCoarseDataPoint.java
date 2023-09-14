package alluxio.stress.worker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

/**
 * One coarse data point captures a series of data points.
 * All data points are group by thread and time slices.
 */
// TODO(tongyu): compress data points in a coarse data point summary
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

  /**
   * Create a coarse data point.
   *
   * @param workerID the ID of the worker
   * @param threadID the ID of the thread
   * @param data the list of data point lists
   * @param throughput the list of throughput
   */
  public WorkerBenchCoarseDataPoint(Long workerID, Long threadID,
                                    List<List<WorkerBenchDataPoint>> data,
                                    List<Long> throughput) {
    mWid = workerID;
    mTid = threadID;
    mData = data;
    mThroughput = throughput;
  }

  /**
   * @return the ID of the worker
   */
  public Long getWid() {
    return mWid;
  }

  /**
   * @param wid the ID of the worker
   */
  public void setWid(Long wid) {
    mWid = wid;
  }

  /**
   * @return the ID of the thread
   */
  public Long getTid() {
    return mTid;
  }

  /**
   * @param tid the ID of the thread
   */
  public void setTid(Long tid) {
    mTid = tid;
  }

  /**
   * @return the list of data point lists
   */
  public List<List<WorkerBenchDataPoint>> getData() {
    return mData;
  }

  /**
   * @param data the list of data point lists
   */
  public void setData(List<List<WorkerBenchDataPoint>> data) {
    mData = data;
  }

  /**
   * @param data add a data point list to the list of data point lists
   */
  public void addDataPoints(List<WorkerBenchDataPoint> data) {
    mData.add(data);
  }

  /**
   * @return the list of all throughput
   */
  public List<Long> getThroughput() {
    return mThroughput;
  }

  /**
   * @param throughput the list of all throughput
   */
  public void setThroughput(List<Long> throughput) {
    mThroughput = throughput;
  }

  /**
   * remove the list of all throughput after worker aggregation.
   */
  public void clearThroughput() {
    mThroughput.clear();
  }
}
