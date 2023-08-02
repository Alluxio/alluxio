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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;

/**
 * One data point captures the information we collect from one I/O operation to a worker.
 * The one operation may be a full scan or positioned read on a file.
 */
@JsonDeserialize(using = WorkerBenchDataPointDeserializer.class)
public class WorkerBenchDataPoint {
  @JsonProperty("workerID")
  public String mWorkerID;
  @JsonProperty("threadID")
  public long mThreadID;

  @JsonProperty("duration")
  public long mDuration;
  @JsonProperty("start")
  public long mStartMs;
  @JsonProperty("ioBytes")
  public long mIOBytes;

  /**
   * @param workerID the worker this I/O operation reads
   * @param threadID the thread performing the I/O
   * @param startMs start timestamp of the I/O
   * @param duration duration of the file read operation
   * @param ioBytes bytes read
   */
  @JsonCreator
  public WorkerBenchDataPoint(@JsonProperty("workerID") String workerID,
                              @JsonProperty("threadID") long threadID,
                              @JsonProperty("start") long startMs,
                              @JsonProperty("duration") long duration,
                              @JsonProperty("ioBytes") long ioBytes) {
    mWorkerID = workerID;
    mThreadID = threadID;
    mStartMs = startMs;
    mDuration = duration;
    mIOBytes = ioBytes;
  }

  /**
   * @return worker ID
   */
  public String getWorkerID() {
    return mWorkerID;
  }

  /**
   * @return thread ID
   */
  public long getThreadID() {
    return mThreadID;
  }

  /**
   * @return duration in ms
   */
  public long getDuration() {
    return mDuration;
  }

  /**
   * @return start timestamp in long
   */
  public long getStartMs() {
    return mStartMs;
  }

  /**
   * @return bytes read
   */
  public long getIOBytes() {
    return mIOBytes;
  }

  /**
   * @param workerID worker ID
   */
  public void setWorkerID(String workerID) {
    mWorkerID = workerID;
  }

  /**
   * @param threadID the thread ID
   */
  public void setThreadID(long threadID) {
    mThreadID = threadID;
  }

  /**
   * @param duration duration in ms
   */
  public void setDuration(long duration) {
    mDuration = duration;
  }

  /**
   * @param startMs start timestamp in long
   */
  public void setStartMs(long startMs) {
    mStartMs = startMs;
  }

  /**
   * @param ioBytes bytes read
   */
  public void setIOBytes(long ioBytes) {
    mIOBytes = ioBytes;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("threadID", mThreadID)
            .add("ioBytes", mIOBytes)
            .add("duration", mDuration)
            .toString();
  }
}
