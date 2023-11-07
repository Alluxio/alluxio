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

/**
 * WorkerBenchMode, HASH or LOCAL_ONLY.
 */
public enum WorkerBenchMode {
  HASH("HASH"),
  LOCAL_ONLY("LOCAL_ONLY");

  private final String mName;

  /**
   * Constructor.
   *
   * @param name of the client type
   */
  WorkerBenchMode(String name) {
    mName = name;
  }

  @Override
  public String toString() {
    return mName;
  }
}
