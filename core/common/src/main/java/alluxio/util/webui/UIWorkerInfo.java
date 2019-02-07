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

package alluxio.util.webui;

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Displays information about a worker in the UI.
 */
public class UIWorkerInfo {
  private final String mWorkerAddress;
  private final long mStartTimeMs;
  private final String mStartTime;

  /**
   * Creates a new instance of {@link UIWorkerInfo}.
   *
   * @param workerAddress worker address
   * @param startTimeMs start time in milliseconds
   * @param dateFormatPattern The pattern to format timestamps with
   */
  public UIWorkerInfo(String workerAddress, long startTimeMs, String dateFormatPattern) {
    mWorkerAddress = workerAddress;
    mStartTimeMs = startTimeMs;
    mStartTime = CommonUtils.convertMsToDate(mStartTimeMs, dateFormatPattern);
  }

  /**
   * Instantiates a new Ui worker info.
   *
   * @param workerAddress the worker address
   * @param startTime the start time
   */
  @JsonCreator
  public UIWorkerInfo(@JsonProperty("workerAddress") String workerAddress,
      @JsonProperty("startTime") String startTime) {
    mWorkerAddress = workerAddress;
    mStartTime = startTime;
    mStartTimeMs = System.currentTimeMillis();
  }

  /**
   * @return the start time
   */
  public String getStartTime() {
    return mStartTime;
  }

  /**
   * @return the worker address
   */
  public String getWorkerAddress() {
    return mWorkerAddress;
  }
}
