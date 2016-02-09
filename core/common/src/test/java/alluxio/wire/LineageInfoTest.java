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

package alluxio.wire;

import com.google.common.collect.Lists;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class LineageInfoTest {

  @Test
  public void jsonTest() throws Exception {
    LineageInfo lineageInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    LineageInfo other = mapper.readValue(mapper.writeValueAsBytes(lineageInfo), LineageInfo.class);
    checkEquality(lineageInfo, other);
  }

  @Test
  public void thriftTest() {
    LineageInfo lineageInfo = createRandom();
    LineageInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(lineageInfo));
    checkEquality(lineageInfo, other);
  }

  public void checkEquality(LineageInfo a, LineageInfo b) {
    Assert.assertEquals(a.getId(), b.getId());
    Assert.assertEquals(a.getInputFiles(), b.getInputFiles());
    Assert.assertEquals(a.getOutputFiles(), b.getOutputFiles());
    Assert.assertEquals(a.getJob(), b.getJob());
    Assert.assertEquals(a.getCreationTimeMs(), b.getCreationTimeMs());
    Assert.assertEquals(a.getParents(), b.getParents());
    Assert.assertEquals(a.getChildren(), b.getChildren());
    Assert.assertEquals(a, b);
  }

  public static LineageInfo createRandom() {
    LineageInfo result = new LineageInfo();
    Random random = new Random();

    long id = random.nextLong();
    byte[] bytes = new byte[5];
    List<String> inputFiles = Lists.newArrayList();
    long numInputFiles = random.nextInt(10) + 1;
    for (int i = 0; i < numInputFiles; i ++) {
      random.nextBytes(bytes);
      inputFiles.add(new String(bytes));
    }
    List<String> outputFiles = Lists.newArrayList();
    long numOutputFiles = random.nextInt(10) + 1;
    for (int i = 0; i < numOutputFiles; i ++) {
      random.nextBytes(bytes);
      outputFiles.add(new String(bytes));
    }
    CommandLineJobInfo job = CommandLineJobInfoTest.createRandom();
    long creationTimeMs = random.nextLong();
    List<Long> parents = Lists.newArrayList();
    long numParents = random.nextInt(10) + 1;
    for (int i = 0; i < numParents; i ++) {
      parents.add(random.nextLong());
    }
    List<Long> children = Lists.newArrayList();
    long numChildren = random.nextInt(10) + 1;
    for (int i = 0; i < numChildren; i ++) {
      children.add(random.nextLong());
    }

    result.setId(id);
    result.setInputFiles(inputFiles);
    result.setOutputFiles(outputFiles);
    result.setJob(job);
    result.setCreationTimeMs(creationTimeMs);
    result.setParents(parents);
    result.setChildren(children);

    return result;
  }
}
