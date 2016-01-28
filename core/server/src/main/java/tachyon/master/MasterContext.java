/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master;

import tachyon.conf.TachyonConf;

/**
 * A MasterContext object stores {@link TachyonConf}.
 */
public final class MasterContext {
  private MasterContext() {} // to prevent initialization

  /**
   * The static configuration object. There is only one {@link TachyonConf} object shared within
   * the same master process.
   */
  private static TachyonConf sTachyonConf = new TachyonConf();

  /**
   * The {@link MasterSource} for collecting master metrics.
   */
  private static MasterSource sMasterSource = new MasterSource();

  /**
   * Returns the one and only static {@link TachyonConf} object which is shared among all classes
   * within the master process.
   *
   * @return the {@link TachyonConf} for the master process
   */
  public static TachyonConf getConf() {
    return sTachyonConf;
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
   * TODO(binfan): consider a better way to mock test TachyonConf
   */
  public static void reset() {
    reset(new TachyonConf());
  }

  /**
   * Resets the master context, for test only.
   * TODO(binfan): consider a better way to mock test TachyonConf
   *
   * @param conf the configuration for Tachyon
   */
  public static void reset(TachyonConf conf) {
    sTachyonConf = conf;
    sMasterSource = new MasterSource();
  }
}
