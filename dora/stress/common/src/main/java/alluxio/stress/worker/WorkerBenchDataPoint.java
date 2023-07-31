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

    public String getWorkerID() {
        return mWorkerID;
    }

    public long getThreadID() {
        return mThreadID;
    }

    public long getDuration() {
        return mDuration;
    }

    public long getStartMs() {
        return mStartMs;
    }

    public long getIOBytes(){
        return mIOBytes;
    }

    public void setWorkerID(String workerID) {
        mWorkerID = workerID;
    }

    public void setThreadID(long threadID) {
        mThreadID = threadID;
    }

    public void setDuration(long duration) {
        mDuration = duration;
    }

    public void setStartMs(long startMs) {
        mStartMs = startMs;
    }

    public void setIOBytes(long ioBytes) {
        mIOBytes = ioBytes;
    }
}
