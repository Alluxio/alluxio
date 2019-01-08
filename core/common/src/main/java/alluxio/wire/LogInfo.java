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

package alluxio.wire;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about the LogInfo.
 */
@NotThreadSafe
public final class LogInfo {

  private String mLevel;
  private String mLogName;
  private String mMessage;

  /**
   * Creates a new instance of {@link LogInfo}.
   */
  public LogInfo(){
  }

  /**
   * @return the level of the log
   */
  public String getLevel() {
    return mLevel;
  }

  /**
   * @param level the log's level
   */
  public void setLevel(String level) {
    mLevel = level;
  }

  /**
   * @return the logger's name
   */
  public String getLogName() {
    return mLogName;
  }

  /**
   * @param logName the logger's name
   */
  public void setLogName(String logName) {
    mLogName = logName;
  }

  /**
   * @return the message
   */
  public String getMessage() {
    return mMessage;
  }

  /**
   * @param message the message
   */
  public void setMessage(String message) {
    mMessage = message;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).addValue(mLevel).addValue(mLogName)
        .addValue((mMessage != null ? mMessage : "")).toString();
  }
}
