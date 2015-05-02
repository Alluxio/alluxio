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

package tachyon.metrics;

import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class MetricsConfigTest {
  private String mFilePath;

  @Before
  public final void Before() {
    mFilePath = getClass().getClassLoader().getResource("test_metrics.properties").getFile();
  }

  @Test
  public void setPropertiesTest() {
    MetricsConfig config = new MetricsConfig(mFilePath);

    Properties masterProp = config.getInstanceProperties("master");
    Assert.assertEquals(3, masterProp.size());
    Assert.assertEquals("tachyon.metrics.sink.ConsoleSink",
        masterProp.getProperty("sink.console.class"));
    Assert.assertEquals("20", masterProp.getProperty("sink.console.period"));
    Assert.assertEquals("minutes", masterProp.getProperty("sink.console.unit"));

    Properties workerProp = config.getInstanceProperties("worker");
    Assert.assertEquals(2, workerProp.size());
    Assert.assertEquals("tachyon.metrics.sink.ConsoleSink",
        workerProp.getProperty("sink.console.class"));
    Assert.assertEquals("15", workerProp.getProperty("sink.console.period"));
  }

  @Test
  public void subPropertiesTest() {
    MetricsConfig config = new MetricsConfig(mFilePath);

    Map<String, Properties> propertyCategories = config.getPropertyCategories();
    Assert.assertEquals(2, propertyCategories.size());

    Properties masterProp = config.getInstanceProperties("master");

    Map<String, Properties> sinkProps = config.subProperties(masterProp, MetricsSystem.SINK_REGEX);
    Assert.assertEquals(1, sinkProps.size());
    Assert.assertTrue(sinkProps.containsKey("console"));

    Properties consoleProp = sinkProps.get("console");
    Assert.assertEquals(3, consoleProp.size());
    Assert.assertEquals("tachyon.metrics.sink.ConsoleSink", consoleProp.getProperty("class"));
  }
}
