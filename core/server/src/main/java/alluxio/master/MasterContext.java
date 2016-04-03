/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.Configuration;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A MasterContext object stores {@link Configuration}.
 */
@ThreadSafe
public final class MasterContext {
  private MasterContext() {} // to prevent initialization

  /**
   * The static configuration object. There is only one {@link Configuration} object shared within
   * the same master process.
   */
  private static Configuration sConfiguration = new Configuration();

  /**
   * The {@link MasterSource} for collecting master metrics.
   */
  private static MasterSource sMasterSource = new MasterSource();

  /**
   * Returns the one and only static {@link Configuration} object which is shared among all classes
   * within the master process.
   *
   * @return the {@link Configuration} for the master process
   */
  public static Configuration getConf() {
    return sConfiguration;
  }

  /**
   * Returns the one and only static {@link MasterSource} object which is shared among all classes
   * within the master process.
   *
   * @return the {@link MasterSource} for the master process
   */
  public static MasterSource getMasterSource() {
    return sMasterSource;
  }

  /**
   * Resets the master context, for test only.
   * TODO(binfan): consider a better way to mock test configuration
   */
  public static void reset() {
    reset(new Configuration());
  }

  /**
   * Resets the master context, for test only.
   * TODO(binfan): consider a better way to mock test configuration
   *
   * @param conf the configuration for Alluxio
   */
  public static void reset(Configuration conf) {
    sConfiguration = conf;
    sMasterSource = new MasterSource();
  }
}
