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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import alluxio.perf.basic.PerfThread;
import alluxio.perf.basic.TaskConfiguration;
import alluxio.perf.benchmark.SimpleTaskContext;

public class SimpleTaskContextTest {
  private final String mJavaTmpDir = System.getProperty("java.io.tmpdir");

  @After
  public final void after() {
    File contextFile = new File(mJavaTmpDir + "/alluxio-perf-test/context-test");
    contextFile.delete();
  }

  @Before
  public final void before() throws IOException {
    File tmpDir = new File(mJavaTmpDir + "/alluxio-perf-test");
    tmpDir.mkdirs();
  }

  @Test
  public void writeLoadTest() throws IOException {
    TaskConfiguration taskConf = TaskConfiguration.get("Foo", false);
    SimpleTaskContext context1 = new SimpleTaskContext();
    context1.initialSet(0, "", "Foo", taskConf);
    context1.setFromThread(new PerfThread[1]);
    File contextFile = new File(mJavaTmpDir + "/alluxio-perf-test/context-test");
    context1.writeToFile(contextFile);
    SimpleTaskContext context2 = new SimpleTaskContext();
    context2.loadFromFile(contextFile);

    Assert.assertEquals(context1.getTestCase(), context2.getTestCase());
    Assert.assertEquals(context1.getId(), context2.getId());
    Assert.assertEquals(context1.getNodeName(), context2.getNodeName());
    Assert.assertEquals(context1.getSuccess(), context2.getSuccess());
    Assert.assertEquals(context1.getStartTimeMs(), context2.getStartTimeMs());
    Assert.assertEquals(context1.getFinishTimeMs(), context2.getFinishTimeMs());

    Assert.assertEquals(context1.mConf, context2.mConf);
    Assert.assertEquals(context1.mAdditiveStatistics, context2.mAdditiveStatistics);
  }
}
