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

package alluxio.worker.http.vo;

import com.google.gson.annotations.SerializedName;

/**
 * The View Object of the Load Job Submit Response.
 */
public class LoadJobSubmitResponseVO {
  @SerializedName("success")
  private boolean mSuccess;

  @SerializedName("jobId")
  private String mJobId;

  @SerializedName("path")
  private String mPath;

  @SerializedName("message")
  private String mMessage;

  /**
   * The default constructor of creating View Object of the Load Job Submit Response.
   */
  public LoadJobSubmitResponseVO() {
  }

  /**
   * The constructor of creating View Object of the Load Job submit.
   * @param success if the job succeed
   * @param jobId the job id
   * @param path the load path
   * @param message the response message
   */
  public LoadJobSubmitResponseVO(boolean success, String jobId, String path, String message) {
    mSuccess = success;
    mJobId = jobId;
    mPath = path;
    mMessage = message;
  }

  /**
   * If the job succeed.
   * @return if the job succeed
   */
  public boolean isSuccess() {
    return mSuccess;
  }

  /**
   * Set if the job succeed.
   * @param success if the job succeed
   */
  public void setSuccess(boolean success) {
    mSuccess = success;
  }

  /**
   * Get the job id.
   * @return the job id
   */
  public String getJobId() {
    return mJobId;
  }

  /**
   * Set the job id.
   * @param jobId the job id
   */
  public void setJobId(String jobId) {
    mJobId = jobId;
  }

  /**
   * Get the load path.
   * @return the load path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Set the load path.
   * @param path the load path
   */
  public void setPath(String path) {
    mPath = path;
  }

  /**
   * Get the response message.
   * @return the response message
   */
  public String getMessage() {
    return mMessage;
  }

  /**
   * Set the response message.
   * @param message the response message
   */
  public void setMessage(String message) {
    mMessage = message;
  }
}
