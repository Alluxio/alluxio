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

package alluxio.perf.benchmark.foo;

import alluxio.perf.basic.PerfThread;
import alluxio.perf.basic.TaskConfiguration;

public class FooThread extends PerfThread {
  private boolean mCleanup = false;
  private boolean mSetup = false;
  private boolean mRun = false;

  @Override
  public void run() {
    mRun = true;
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mSetup = taskConf.getBooleanProperty("foo");
    return true;
  }

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf) {
    mCleanup = true;
    return true;
  }

  public boolean getCleanup() {
    return mCleanup;
  }

  public boolean getSetup() {
    return mSetup;
  }

  public boolean getRun() {
    return mRun;
  }
}
