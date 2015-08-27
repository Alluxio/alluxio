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

package tachyon.worker;

import tachyon.conf.TachyonConf;

/**
 * A WorkerContext object stores TachyonConf
 */
public class WorkerContext {
  /**
   * The static configuration object. There is only one TachyonConf object shared within the same
   * worker process.
   */
  private static TachyonConf sTachyonConf = new TachyonConf();

  /**
   * Returns the one and only static {@link TachyonConf} object which is shared among all classes
   * within the worker process
   *
   * @return the tachyonConf for the worker process
   */
  public static TachyonConf getConf() {
    return sTachyonConf;
  }
}
