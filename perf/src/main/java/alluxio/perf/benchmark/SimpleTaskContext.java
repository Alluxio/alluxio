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

package alluxio.perf.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import alluxio.perf.basic.PerfTaskContext;
import alluxio.perf.basic.PerfThread;
import alluxio.perf.basic.TaskConfiguration;

/**
 * An simple example to extend PerfTaskContext. This context maintains additive statistics and can
 * be used to quickly implement a new benchmark.
 */
public class SimpleTaskContext extends PerfTaskContext {
  protected Map<String, String> mConf;

  protected Map<String, List<Double>> mAdditiveStatistics;

  @Override
  public void initialSet(int id, String nodeName, String testCase, TaskConfiguration taskConf) {
    super.initialSet(id, nodeName, testCase, taskConf);
    mConf = taskConf.getAllProperties();
  }

  public Map<String, List<Double>> getAdditiveStatistics() {
    return mAdditiveStatistics;
  }

  public Map<String, String> getConf() {
    return mConf;
  }

  @Override
  public void loadFromFile(File file) throws IOException {
    BufferedReader fin = new BufferedReader(new FileReader(file));
    mTestCase = fin.readLine();
    mId = Integer.parseInt(fin.readLine());
    mNodeName = fin.readLine();
    mSuccess = Boolean.parseBoolean(fin.readLine());
    mStartTimeMs = Long.parseLong(fin.readLine());
    mFinishTimeMs = Long.parseLong(fin.readLine());
    int confSize = Integer.parseInt(fin.readLine());
    mConf = new HashMap<String, String>(confSize);
    for (int i = 0; i < confSize; i ++) {
      mConf.put(fin.readLine(), fin.readLine());
    }
    int sSize = Integer.parseInt(fin.readLine());
    mAdditiveStatistics = new HashMap<String, List<Double>>(sSize);
    for (int i = 0; i < sSize; i ++) {
      String key = fin.readLine();
      int lSize = Integer.parseInt(fin.readLine());
      List<Double> value = new ArrayList<Double>(lSize);
      for (int j = 0; j < lSize; j ++) {
        value.add(Double.parseDouble(fin.readLine()));
      }
      mAdditiveStatistics.put(key, value);
    }
    fin.close();
    return;
  }

  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>();
    List<Double> statistic = new ArrayList<Double>(threads.length);
    for (int i = 0; i < threads.length; i ++) {
      statistic.add(0.0);
    }
    mAdditiveStatistics.put("Statistic", statistic);
  }

  @Override
  public void writeToFile(File file) throws IOException {
    BufferedWriter fout = new BufferedWriter(new FileWriter(file));
    fout.write(mTestCase + "\n");
    fout.write(mId + "\n");
    fout.write(mNodeName + "\n");
    fout.write(mSuccess + "\n");
    fout.write(mStartTimeMs + "\n");
    fout.write(mFinishTimeMs + "\n");
    fout.write(mConf.size() + "\n");
    for (Map.Entry<String, String> entry : mConf.entrySet()) {
      fout.write(entry.getKey() + "\n");
      fout.write(entry.getValue() + "\n");
    }
    fout.write(mAdditiveStatistics.size() + "\n");
    for (Map.Entry<String, List<Double>> entry : mAdditiveStatistics.entrySet()) {
      fout.write(entry.getKey() + "\n");
      List<Double> value = entry.getValue();
      fout.write(value.size() + "\n");
      for (Double ele : value) {
        fout.write(ele + "\n");
      }
    }
    fout.close();
  }
}
