/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.stress.worker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.List;

/**
 * One coarseDataPoint captures and merges the performance of I/O operations in a specified window.
 * The I/O operations are grouped by worker and by thread. In other words, I/O operations in
 * a window in different threads will be recorded in different coarse data points.
 */
@JsonDeserialize(using = WorkerBenchCoarseDataPointDeserializer.class)
public class WorkerBenchCoarseDataPoint {
  @JsonProperty("workerId")
  private Long mWorkerId;
  @JsonProperty("threadId")
  private Long mThreadId;
  @JsonProperty("data")
  private List<WorkerBenchDataPoint> mData;
  @JsonProperty("throughput")
  private List<Long> mThroughput;

  /**
   * Creates a coarse data point without data and throughput arrays.
   *
   * @param workerID the ID of the worker
   * @param threadID the ID of the thread
   */
  public WorkerBenchCoarseDataPoint(Long workerID, Long threadID) {
    mWorkerId = workerID;
    mThreadId = threadID;
    mData = new ArrayList<>();
    mThroughput = new ArrayList<>();
  }

  /**
   * Creates a coarse data point with data and throughput arrays.
   *
   * @param workerID the ID of the worker
   * @param threadID the ID of the thread
   * @param data the list of data point lists
   * @param throughput the list of throughput
   */
  public WorkerBenchCoarseDataPoint(Long workerID, Long threadID,
                                    List<WorkerBenchDataPoint> data,
                                    List<Long> throughput) {
    mWorkerId = workerID;
    mThreadId = threadID;
    mData = data;
    mThroughput = throughput;
  }

  /**
   * @return the ID of the worker
   */
  public Long getWid() {
    return mWorkerId;
  }

  /**
   * @param wid the ID of the worker
   */
  public void setWid(Long wid) {
    mWorkerId = wid;
  }

  /**
   * @return the ID of the thread
   */
  public Long getTid() {
    return mThreadId;
  }

  /**
   * @param tid the ID of the thread
   */
  public void setTid(Long tid) {
    mThreadId = tid;
  }

  /**
   * @return the list of data point lists
   */
  public List<WorkerBenchDataPoint> getData() {
    return mData;
  }

  /**
   * @param data the list of data point lists
   */
  public void setData(List<WorkerBenchDataPoint> data) {
    mData = data;
  }

  /**
   * @param data add a data point list to the list of data point lists
   */
  public void addDataPoint(WorkerBenchDataPoint data) {
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
   * removes the list of all throughput after worker aggregation.
   */
  public void clearThroughput() {
    mThroughput.clear();
  }
}
