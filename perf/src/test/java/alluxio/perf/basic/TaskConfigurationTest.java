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

import alluxio.perf.basic.TaskConfiguration;

public class TaskConfigurationTest {
  private final String mJavaTmpDir = System.getProperty("java.io.tmpdir");

  @After
  public final void after() {
    File xmlFile = new File(mJavaTmpDir + "/alluxio-perf-test/conf/testsuite/Foo.xml");
    xmlFile.delete();
  }

  @Before
  public final void before() throws IOException {
    File tmpDir = new File(mJavaTmpDir + "/alluxio-perf-test/conf/testsuite");
    tmpDir.mkdirs();
    File xmlFile = new File(mJavaTmpDir + "/alluxio-perf-test/conf/testsuite/Foo.xml");
    xmlFile.delete();
    BufferedWriter fout = new BufferedWriter(new FileWriter(xmlFile));
    fout.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    fout.write("<configuration>\n");
    fout.write("<property>\n");
    fout.write("<name>xxx</name>\n");
    fout.write("<value>yyy</value>\n");
    fout.write("</property>\n");
    fout.write("</configuration>\n");
    fout.close();

    System.setProperty("alluxio.perf.home", mJavaTmpDir + "/alluxio-perf-test");
  }

  @Test
  public void parseFileTest() throws Exception {
    TaskConfiguration taskConf =
        new TaskConfiguration(mJavaTmpDir + "/alluxio-perf-test/conf/testsuite/Foo.xml");
    Assert.assertEquals(1, taskConf.getAllProperties().size());
    Assert.assertEquals("yyy", taskConf.getProperty("xxx"));
  }

  @Test
  public void getSetTest() {
    TaskConfiguration taskConf = new TaskConfiguration();
    taskConf.addProperty("pro.int", "123");
    taskConf.addProperty("pro.boolean", "true");
    taskConf.addProperty("pro.long", "9876543210");
    taskConf.addProperty("pro.string", "foo");
    Assert.assertEquals(123, taskConf.getIntProperty("pro.int"));
    Assert.assertTrue(taskConf.getBooleanProperty("pro.boolean"));
    Assert.assertEquals(9876543210L, taskConf.getLongProperty("pro.long"));
    Assert.assertEquals("foo", taskConf.getProperty("pro.string"));
  }
}
