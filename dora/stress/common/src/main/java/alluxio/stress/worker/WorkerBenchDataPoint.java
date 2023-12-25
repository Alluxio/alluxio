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
  @JsonProperty("count")
  public long mCount;
  @JsonProperty("iobytes")
  public long mIOBytes;

  /**
   * @param count number of files read
   * @param ioBytes total bytes read
   */
  @JsonCreator
  public WorkerBenchDataPoint(long count, long ioBytes) {
    mCount = count;
    mIOBytes = ioBytes;
  }

  /**
   * A constructor without parameters.
   */
  public WorkerBenchDataPoint() {
    mCount = 0;
    mIOBytes = 0;
  }
}
