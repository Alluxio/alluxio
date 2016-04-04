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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import alluxio.perf.benchmark.ListGenerator;

public class ListGeneratorTest {
  @Test
  public void generateRandomReadFilesTest() {
    int filesNum = 5;
    List<String> candidates = new ArrayList<String>(20);
    for (int i = 0; i < 20; i ++) {
      candidates.add("c" + i);
    }

    List<String> result = ListGenerator.generateRandomReadFiles(filesNum, candidates);
    Assert.assertEquals(filesNum, result.size());
    for (String s : result) {
      Assert.assertTrue(candidates.contains(s));
    }
  }

  @Test
  public void generateSequenceReadFilesTest() {
    int threadsNum = 4;
    int filesPerThread = 6;
    List<String> candidates = new ArrayList<String>(20);
    for (int i = 0; i < 20; i ++) {
      candidates.add(i + "");
    }
    for (int i = 0; i < threadsNum; i ++) {
      List<String> result =
          ListGenerator.generateSequenceReadFiles(i, threadsNum, filesPerThread, candidates);
      Assert.assertEquals(filesPerThread, result.size());
      for (int j = 0; j < filesPerThread; j ++) {
        String expect = (20 / threadsNum * i + j) % 20 + "";
        Assert.assertEquals(expect, result.get(j));
      }
    }
  }

  @Test
  public void generateWriteFilesTest() {
    int id = 3;
    int filesNum = 6;
    String dirPrefix = "xyz";
    List<String> result = ListGenerator.generateWriteFiles(id, filesNum, dirPrefix);
    Assert.assertEquals(filesNum, result.size());
    for (int i = 0; i < filesNum; i ++) {
      Assert.assertEquals("xyz/" + id + "-" + i, result.get(i));
    }
  }
}
