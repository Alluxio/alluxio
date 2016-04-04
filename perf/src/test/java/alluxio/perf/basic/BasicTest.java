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

package alluxio.perf.basic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import alluxio.perf.basic.PerfTask;
import alluxio.perf.basic.PerfTaskContext;
import alluxio.perf.basic.PerfTotalReport;
import alluxio.perf.basic.TaskConfiguration;
import alluxio.perf.basic.TestCase;
import alluxio.perf.benchmark.foo.FooTaskContext;
import alluxio.perf.benchmark.foo.FooTotalReport;

public class BasicTest {
  private final String mJavaTmpDir = System.getProperty("java.io.tmpdir");

  @After
  public final void after() {
    File xmlFile = new File(mJavaTmpDir + "/alluxio-perf-test/conf/test-case.xml");
    xmlFile.delete();
  }

  @Before
  public final void before() throws IOException {
    File tmpDir = new File(mJavaTmpDir + "/alluxio-perf-test/conf");
    tmpDir.mkdirs();
    File xmlFile = new File(mJavaTmpDir + "/alluxio-perf-test/conf/test-case.xml");
    xmlFile.delete();
    BufferedWriter fout = new BufferedWriter(new FileWriter(xmlFile));
    fout.write("<testCases>\n");
    fout.write("<type>\n");
    fout.write("<name>Foo</name>\n");
    fout.write("<taskClass>alluxio.perf.benchmark.foo.FooTask</taskClass>\n");
    fout.write("<taskContextClass>alluxio.perf.benchmark.foo.FooTaskContext</taskContextClass>\n");
    fout.write("<taskThreadClass>alluxio.perf.benchmark.foo.FooThread</taskThreadClass>\n");
    fout.write("<totalReportClass>alluxio.perf.benchmark.foo.FooTotalReport</totalReportClass>\n");
    fout.write("</type>\n");
    fout.write("</testCases>\n");
    fout.close();

    System.setProperty("alluxio.perf.home", mJavaTmpDir + "/alluxio-perf-test");
  }

  @Test
  public void taskWorkflowTest() throws Exception {
    String nodeName = "testNode";
    int taskId = 0;
    int totalTasks = 1;
    String testCase = "Foo";

    TaskConfiguration taskConf = TaskConfiguration.get(testCase, false);
    PerfTask task = TestCase.get().getTaskClass(testCase);
    task.initialSet(taskId, totalTasks, nodeName, taskConf, testCase);
    Assert.assertEquals(taskId, task.mId);
    Assert.assertEquals(totalTasks, task.mTotalTasks);
    Assert.assertEquals(nodeName, task.mNodeName);
    Assert.assertEquals(testCase, task.mTestCase);
    Assert.assertEquals("Foo", task.getCleanupDir());

    PerfTaskContext taskContext = TestCase.get().getTaskContextClass(testCase);
    taskContext.initialSet(taskId, nodeName, testCase, taskConf);
    Assert.assertEquals(taskId, taskContext.mId);
    Assert.assertEquals(nodeName, taskContext.mNodeName);
    Assert.assertEquals(testCase, taskContext.mTestCase);
    Assert.assertTrue(taskContext.mSuccess);

    Assert.assertTrue(task.setup(taskContext));
    Assert.assertTrue(task.run(taskContext));
    Assert.assertTrue(task.cleanup(taskContext));

    Assert.assertEquals(5, ((FooTaskContext) taskContext).getFoo());
    Assert.assertTrue(((FooTaskContext) taskContext).getThreads());
    Assert.assertTrue(((FooTaskContext) taskContext).getWrite());

    PerfTotalReport totalReport = TestCase.get().getTotalReportClass(testCase);
    totalReport.initialSet(testCase);
    Assert.assertEquals(testCase, totalReport.mTestCase);

    PerfTaskContext[] taskContexts = new PerfTaskContext[3];
    for (int i = 0; i < taskContexts.length; i ++) {
      taskContexts[i] = taskContext;
    }
    totalReport.initialFromTaskContexts(taskContexts);
    totalReport.writeToFile(null);
    Assert.assertEquals(15, ((FooTotalReport) totalReport).getFoo());
    Assert.assertTrue(((FooTotalReport) totalReport).getWrite());
  }
}
