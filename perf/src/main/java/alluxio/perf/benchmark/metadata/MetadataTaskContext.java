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

package alluxio.perf.benchmark.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import alluxio.perf.basic.PerfThread;
import alluxio.perf.benchmark.SimpleTaskContext;

public class MetadataTaskContext extends SimpleTaskContext {
  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>(1);
    List<Double> rates = new ArrayList<Double>(threads.length);
    for (PerfThread thread : threads) {
      if (!((MetadataThread) thread).getSuccess()) {
        mSuccess = false;
      }
      rates.add(((MetadataThread) thread).getRate());
    }
    mAdditiveStatistics.put("ResponseRate(ops/sec)", rates);
  }
}
