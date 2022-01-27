/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.stress.cli;

import java.util.HashMap;

/**
 * A tool to run multiple tasks for StressBench.
 */
public class PlanRunner {
  private static String[] sMasterPlan = {"MasterOpenFilePlan", "MasterGetBlockLocationsPlan",
      "MasterGetFileStatusPlan", "MasterRenameFilePlan", "MasterListDirPlan",
      "MasterDeleteFilePlan", "MasterComprehensiveFileTestPlan"};

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    int type = getPlanType(args[0]);
    BenchPlan plan;
    switch (type) {
      case 1 :
        plan = new MasterBenchPlan();
        break;
      default:
        System.out.println("Please input a valid plan");
        return;
    }
    plan.run(args);
  }

  private static int getPlanType(String plan) {
    HashMap<String, Integer> map = new HashMap<>();
    for (String s : sMasterPlan) {
      map.put(s, 1);
    }
    return map.getOrDefault(plan, 0);
  }
}
