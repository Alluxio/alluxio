/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.wire;

import alluxio.util.CommonUtils;

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
    List<String> inputFiles = Lists.newArrayList();
    long numInputFiles = random.nextInt(10);
    for (int i = 0; i < numInputFiles; i++) {
      inputFiles.add(CommonUtils.randomString(random.nextInt(10)));
    }
    List<String> outputFiles = Lists.newArrayList();
    long numOutputFiles = random.nextInt(10);
    for (int i = 0; i < numOutputFiles; i++) {
      outputFiles.add(CommonUtils.randomString(random.nextInt(10)));
    }
    CommandLineJobInfo job = CommandLineJobInfoTest.createRandom();
    long creationTimeMs = random.nextLong();
    List<Long> parents = Lists.newArrayList();
    long numParents = random.nextInt(10);
    for (int i = 0; i < numParents; i++) {
      parents.add(random.nextLong());
    }
    List<Long> children = Lists.newArrayList();
    long numChildren = random.nextInt(10);
    for (int i = 0; i < numChildren; i++) {
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
