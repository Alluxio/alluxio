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

import java.io.File;
import java.io.IOException;

import alluxio.perf.basic.PerfTaskContext;
import alluxio.perf.basic.PerfThread;

public class FooTaskContext extends PerfTaskContext {
  private int mFoo = 0;
  private boolean mLoad = false;
  private boolean mThreads = false;
  private boolean mWrite = false;

  @Override
  public void loadFromFile(File file) throws IOException {
    mLoad = true;
  }

  @Override
  public void setFromThread(PerfThread[] threads) {
    mThreads = true;
    for (PerfThread thread : threads) {
      mThreads &= ((FooThread) thread).getSetup();
      mThreads &= ((FooThread) thread).getRun();
      mThreads &= ((FooThread) thread).getCleanup();
    }
  }

  @Override
  public void writeToFile(File file) throws IOException {
    mWrite = true;
  }

  public int getFoo() {
    return mFoo;
  }

  public boolean getLoad() {
    return mLoad;
  }

  public boolean getThreads() {
    return mThreads;
  }

  public boolean getWrite() {
    return mWrite;
  }

  public void setFoo(int foo) {
    mFoo = foo;
  }
}
