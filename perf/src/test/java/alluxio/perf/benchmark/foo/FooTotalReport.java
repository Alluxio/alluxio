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
import alluxio.perf.basic.PerfTotalReport;

public class FooTotalReport extends PerfTotalReport {
  private int mFoo = 0;
  private boolean mWrite = false;

  @Override
  public void initialFromTaskContexts(PerfTaskContext[] taskContexts) throws IOException {
    for (PerfTaskContext taskContext : taskContexts) {
      mFoo += ((FooTaskContext) taskContext).getFoo();
    }
  }

  @Override
  public void writeToFile(File file) throws IOException {
    mWrite = true;
  }

  public int getFoo() {
    return mFoo;
  }

  public boolean getWrite() {
    return mWrite;
  }
}
