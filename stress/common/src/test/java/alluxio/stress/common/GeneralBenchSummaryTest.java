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

package alluxio.stress.common;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import alluxio.stress.BaseParameters;
import alluxio.stress.GraphGenerator;
import alluxio.stress.TaskResult;

import org.junit.Test;

import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;

public class GeneralBenchSummaryTest {
  // Add a test implementation to test the abstract parent
  private class TestTaskResult implements TaskResult {
    private BaseParameters mBaseParameters;
    private List<String> mErrors;

    TestTaskResult() {
      mBaseParameters = new BaseParameters();
      mErrors = new ArrayList<>();
    }

    @Override
    public Aggregator aggregator() {
      return null;
    }

    @Override
    public BaseParameters getBaseParameters() {
      return mBaseParameters;
    }

    @Override
    public List<String> getErrors() {
      return mErrors;
    }

    public void addErrors(String s) {
      mErrors.add(s);
    }
  }

  private class TestMultipleNodeSummary extends GeneralBenchSummary<TestTaskResult> {
    @Override
    public GraphGenerator graphGenerator() {
      return null;
    }

    public void addTaskResultWithErrors(int n) {
      int index = mNodeResults.size();
      for (int i = 0; i < n; i++) {
        TestTaskResult result = new TestTaskResult();
        String taskID = "task" + index;
        result.getBaseParameters().mId = taskID;
        result.addErrors("error" + index);
        mNodeResults.put(taskID, result);
        index++;
      }
    }

    public void addTaskResultWithoutErrors(int n) {
      int index = mNodeResults.size();
      for (int i = 0; i < n; i++) {
        TestTaskResult result = new TestTaskResult();
        String taskID = "task" + index;
        result.getBaseParameters().mId = taskID;
        mNodeResults.put(taskID, result);
        index++;
      }
    }
  }

  @Test
  public void collectErrorFromAllNodesWithEmptyResults() {
    // test summary with empty nodes
    TestMultipleNodeSummary summary = new TestMultipleNodeSummary();
    List<String> emptyList = summary.collectErrorsFromAllNodes();
    assertTrue(emptyList.isEmpty());
  }

  @Test
  public void collectErrorFromAllNodesWithNoErrors() {
    // test summary with node and no error
    TestMultipleNodeSummary summary = new TestMultipleNodeSummary();
    summary.addTaskResultWithoutErrors(6);
    List<String> emptyList = summary.collectErrorsFromAllNodes();
    assertTrue(emptyList.isEmpty());
  }

  @Test
  public void collectErrorFromAllNodesWithErrors() {
    // test summary with errors
    TestMultipleNodeSummary summary = new TestMultipleNodeSummary();

    summary.addTaskResultWithoutErrors(4);
    summary.addTaskResultWithErrors(3);

    List<String> list = summary.collectErrorsFromAllNodes();
    assertEquals(list.size(), 3);
    Set<String> set = new HashSet<>(list);
    for (int i = 4; i < 6; i++) {
      String message = String.format("task%s :error%s", i, i);
      assertTrue(set.contains(message));
    }
  }
}
