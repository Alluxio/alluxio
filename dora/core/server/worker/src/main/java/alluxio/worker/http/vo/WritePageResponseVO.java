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
 * The View Object of the Write Page Response.
 */
public class WritePageResponseVO {

  @SerializedName("success")
  private boolean mSuccess;

  @SerializedName("message")
  private String mMessage;

  /**
   * The default constructor of creating View Object of the Write Page Response.
   */
  public WritePageResponseVO() {
  }

  /**
   * The constructor of creating View Object of the Write Page Response.
   * @param success if the job succeed
   * @param message the response message
   */
  public WritePageResponseVO(boolean success, String message) {
    mSuccess = success;
    mMessage = message;
  }

  /**
   * If page written successfully.
   * @return if page written successfully
   */
  public boolean isSuccess() {
    return mSuccess;
  }

  /**
   * Set if page written successfully.
   * @param success if page written successfully
   */
  public void setSuccess(boolean success) {
    mSuccess = success;
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
