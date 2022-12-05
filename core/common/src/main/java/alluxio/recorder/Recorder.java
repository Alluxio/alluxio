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
  // To prevent adding too many records, causing memory leaks, set a maximum number of records
  private static final int MAX_RECORDS_COUNT = 10_000;

  private final LinkedList<String> mRecords;

  /**
   * Constructs a new instance.
   */
  public Recorder() {
    mRecords = new LinkedList<>();
  }

  /**
   * Records a message.
   * @param message message
   */
  public void record(String message) {
    mRecords.add(message);
    if (mRecords.size() > MAX_RECORDS_COUNT) {
      mRecords.removeFirst();
    }
  }

  /**
   * Records a message with a format template and arguments.
   * @param format the message format string
   * @param arguments the message string to be recorded
   */
  public void record(String format, Object... arguments) {
    record(MessageFormatter.arrayFormat(format, arguments).getMessage());
  }

  /**
   * Gets and clears the records recorded so far. After this, the recorder is empty.
   * @return the records
   */
  public List<String> takeRecords() {
    List<String> records = ImmutableList.copyOf(mRecords);
    mRecords.clear();
    return records;
  }

  /**
   * Gets a {@link NoopRecorder} that does not actually record anything.
   * @return a NoopRecorder instance
   */
  public static Recorder noopRecorder() {
    return NoopRecorder.INSTANCE;
  }
}
