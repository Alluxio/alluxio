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

package alluxio.recorder;

import org.slf4j.helpers.MessageFormatter;

import java.util.LinkedList;
import java.util.List;

public class Recorder {
  private final List<String> mRecorder;
  private boolean mEnableRecorder;

  private Recorder(List<String> recorder, boolean enable) {
    mRecorder = recorder;
    mEnableRecorder = enable;
  }

  private Recorder(List<String> recorder) {
    this(recorder, false);
  }

  public static Recorder create() {
    return new Recorder(new LinkedList<>());
  }

  /**
   * Record a message.
   * @param message options builder
   */
  private void record(String message) {
    mRecorder.add(message);
  }

  public void setEnable() {
    mEnableRecorder = true;
  }

  /**
   * Record a message.
   * @param message options builder
   */
  public void recordIfEnable(String message) {
    if (mEnableRecorder) {
      record(message);
    }
  }

  /**
   * Record a message.
   * @param format options builder
   * @param arguments options builder
   */
  public void recordIfEnable(String format, Object... arguments) {
    if (mEnableRecorder) {
      record(MessageFormatter.arrayFormat(format, arguments).getMessage());
    }
  }

  public List<String> getRecord() {
    return mRecorder;
  }
}
