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

package alluxio.stress;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import alluxio.stress.cli.AbstractStressBench;
import alluxio.stress.common.FileSystemParameters;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AbstractStressBenchTest {
  // an implementation to test the abstract class logic
  private class TestStressBench extends AbstractStressBench<TaskResult, FileSystemParameters> {
    // used to store the executed write type and arguments
    List<String> mWriteTypeList = new ArrayList<>();
    List<String[]> mArgsList = new ArrayList<>();

    TestStressBench() {
      mParameters = new FileSystemParameters();
    }

    @Override
    public String getBenchDescription() {
      return "";
    }

    @Override
    public TaskResult runLocal() {
      return null;
    }

    @Override
    public void prepare() {
    }

    // runSingleTask will store the executed task arguments
    @Override
    protected String runSingleTask(String[] args) {
      mWriteTypeList.add(mParameters.mWriteType);
      mArgsList.add(args.clone());
      return "";
    }

    public FileSystemParameters getParameter() {
      return mParameters;
    }

    public List<String> getWriteTypeList() {
      return mWriteTypeList;
    }

    public List<String[]> getArgsList() {
      return mArgsList;
    }
  }

  @Test
  public void singleTaskTest() throws Exception {
    String[] input = new String[] {
        "--write-type", "MUST_CACHE",
        "--read-type", "NO_CACHE",
        "--client-type", "AlluxioNative",
    };

    // try all possible write type input
    List<String> possibleWriteType = ImmutableList.of("MUST_CACHE", "CACHE_THROUGH",
        "ASYNC_THROUGH", "THROUGH");

    for (int i = 0; i < possibleWriteType.size(); i++) {
      TestStressBench testBench = new TestStressBench();
      input[1] = possibleWriteType.get(i);
      testBench.run(input);

      List<String> writeTypeList = testBench.getWriteTypeList();
      List<String[]> argsList = testBench.getArgsList();

      // ensure we have only executed one task
      assertEquals(argsList.size(), 1);
      assertEquals(writeTypeList.size(), 1);

      // ensure we have executed the desired input
      assertArrayEquals(input, argsList.get(0));
    }
  }

  @Test
  public void multiTaskTest() throws Exception {
    String[] input = new String[] {
        "--write-type", "ALL",
        "--read-type", "NO_CACHE",
        "--client-type", "AlluxioNative",
    };

    TestStressBench testBench = new TestStressBench();
    testBench.run(input);

    List<String> writeTypeList = testBench.getWriteTypeList();
    List<String[]> argsList = testBench.getArgsList();

    // check the input task is the same as the expected batch task
    List<String> possibleWriteType = ImmutableList.of("MUST_CACHE", "CACHE_THROUGH",
        "ASYNC_THROUGH", "THROUGH");
    assertEquals(argsList.size(), 4);
    for (int i = 0; i < argsList.size(); i++) {
      // compare the mParameter
      assertEquals(possibleWriteType.get(i), writeTypeList.get(i));

      // compare the executed single task arguments
      input[1] = possibleWriteType.get(i);
      String[] executedTask = argsList.get(i);
      assertArrayEquals(input, executedTask);
    }
  }
}
