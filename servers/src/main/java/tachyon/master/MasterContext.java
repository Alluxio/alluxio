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
 * A MasterContext object stores TachyonConf.
 */
public final class MasterContext {
  private MasterContext() {} // to prevent initialization

  /**
   * The static configuration object. There is only one TachyonConf object shared within the same
   * master process.
   */
  private static TachyonConf sTachyonConf = new TachyonConf();

  /**
   * The MasterSource for collecting master metrics.
   */
  private static MasterSource sMasterSource = new MasterSource();

  /**
   * Returns the one and only static {@link tachyon.conf.TachyonConf} object which is shared among
   * all classes within the master process.
   *
   * @return the tachyonConf for the master process
   */
  public static TachyonConf getConf() {
    return sTachyonConf;
  }

  /**
   * Returns the one and only static {@link tachyon.master.MasterSource} object which is shared
   * among all classes within the master process.
   *
   * @return the MasterSource for the master process
   */
  public static MasterSource getMasterSource() {
    return sMasterSource;
  }

  /**
   * Reset the TachyonConf instance in master context, for test only.
   * TODO(binfan): consider a better way to mock test TachyonConf
   */
  public static void resetConf() {
    sTachyonConf = new TachyonConf();
  }
}
