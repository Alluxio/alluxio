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

import java.util.LinkedList;
import java.util.List;

/**
 * A Recorder which does nothing.
 */
public class NoopRecorder extends Recorder {
  private final LinkedList<String> mRecords;
  static final Recorder INSTANCE = new NoopRecorder();

  /**
   * Constructs a new instance.
   */
  private NoopRecorder() {
    super();
    mRecords = new LinkedList<>();
  }

  @Override
  public void record(String message) {}

  @Override
  public void record(String format, Object... arguments) {}

  @Override
  public List<String> takeRecords() {
    return mRecords;
  }
}
