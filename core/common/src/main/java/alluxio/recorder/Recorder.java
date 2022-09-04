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

import com.google.common.collect.ImmutableList;
import org.slf4j.helpers.MessageFormatter;

import java.util.LinkedList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A tool for recording information, which can be used to record the process of execution.
 */
@NotThreadSafe
public class Recorder {
  private final List<String> mRecord;
  private boolean mEnableRecord;

  private Recorder(List<String> record, boolean enable) {
    mRecord = record;
    mEnableRecord = enable;
  }

  /**
   * Create a disabled Recorder Object.
   * By default, mEnableRecorder is false needs to be enabled by {@link Recorder#setEnabled()}.
   * @return A {@code Recorder} Object
   */
  public static Recorder createDisabledRecorder() {
    return new Recorder(new LinkedList<>(), false);
  }

  /**
   * Record a message.
   * @param message options builder
   */
  private void record(String message) {
    mRecord.add(message);
  }

  /**
   * Setting enable Record.
   */
  public void setEnabled() {
    mEnableRecord = true;
  }

  /**
   * Record a message, if the recorder is enabled.
   * @param message the message string to be recorded
   */
  public void recordIfEnabled(String message) {
    if (mEnableRecord) {
      record(message);
    }
  }

  /**
   * Record a message, if the recorder is enabled.
   * @param format the message format string
   * @param arguments the message string to be recorded
   */
  public void recordIfEnabled(String format, Object... arguments) {
    if (mEnableRecord) {
      record(MessageFormatter.arrayFormat(format, arguments).getMessage());
    }
  }

  /**
   * Get a record.
   * @return the record
   */
  public List<String> getRecord() {
    return ImmutableList.copyOf(mRecord);
  }
}
