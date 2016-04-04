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

package alluxio.perf.tools;

import java.io.File;
import java.io.IOException;

import alluxio.perf.basic.PerfTaskContext;
import alluxio.perf.basic.PerfTotalReport;
import alluxio.perf.basic.TestCase;
import alluxio.perf.conf.PerfConf;

/**
 * Generate a total report for the specified test.
 */
public class AlluxioPerfCollector {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Wrong program arguments. Should be <TestCase> <reports dir>");
      System.exit(-1);
    }

    try {
      PerfTotalReport summaryReport = TestCase.get().getTotalReportClass(args[0]);
      summaryReport.initialSet(args[0]);
      File contextsDir = new File(args[1]);
      File[] contextFiles = contextsDir.listFiles();
      if (contextFiles == null || contextFiles.length == 0) {
        throw new IOException("No task context files exists under " + args[1]);
      }
      PerfTaskContext[] taskContexts = new PerfTaskContext[contextFiles.length];
      for (int i = 0; i < contextFiles.length; i ++) {
        taskContexts[i] = TestCase.get().getTaskContextClass(args[0]);
        taskContexts[i].loadFromFile(contextFiles[i]);
      }
      summaryReport.initialFromTaskContexts(taskContexts);
      String outputFileName = PerfConf.get().OUT_FOLDER + "/AlluxioPerfReport-" + args[0];
      summaryReport.writeToFile(new File(outputFileName));
      System.out.println("Report generated at " + outputFileName);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed to generate Alluxio-Perf-Report");
      System.exit(-1);
    }
  }
}
