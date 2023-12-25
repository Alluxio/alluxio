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

import java.util.HashMap;
import java.util.Map;

/**
 * The View Object of Load Job Progress Response.
 */
public class LoadJobProgressResponseVO {
  @SerializedName("jobState")
  private String mJobState;

  @SerializedName("path")
  private String mPath;

  @SerializedName("message")
  private String mMessage;

  @SerializedName("respProperties")
  private Map<String, String> mRespProperties = new HashMap<>();

  /**
   * The default constructor for creating View Object of Load Job Progress Response.
   */
  public LoadJobProgressResponseVO() {
  }

  /**
   * The constructor for creating View Object of Load Job Progress Response.
   * @param jobState
   * @param path the load path to get progress
   * @param message the response message
   * @param respProperties the response properties map parsed from the response message
   */
  public LoadJobProgressResponseVO(String jobState, String path, String message,
                                   Map<String, String> respProperties) {
    mJobState = jobState;
    mPath = path;
    mMessage = message;
    mRespProperties = respProperties;
  }

  /**
   * Get the job state.
   * @return the job state
   */
  public String getJobState() {
    return mJobState;
  }

  /**
   * Set the job state.
   * @param jobState the job state
   */
  public void setJobState(String jobState) {
    mJobState = jobState;
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

  /**
   * Set the response properties map.
   * @param respProperties the response properties map
   */
  public void setRespProperties(Map<String, String> respProperties) {
    mRespProperties = respProperties;
  }

  /**
   * Get the response properties map.
   * @return the response properties map
   */
  public Map<String, String> getRespProperties() {
    return mRespProperties;
  }
}
