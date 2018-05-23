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

package alluxio.conf;

/**
 * The source of a configuration property.
 */
public enum Source {
  UNKNOWN(-1),
  DEFAULT(0),
  SITE_PROPERTY(1),
  SYSTEM_PROPERTY(2),
  HADOOP_CONF(3),
  ;
  private final int mPriority;
  Source(int val) {
    mPriority = val;
  }

  /**
   * @return the priority of the source
   */
  public int getPriority() {
    return mPriority;
  }
}
