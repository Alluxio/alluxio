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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterSource;
import tachyon.worker.WorkerSource;

public class MetricsSystemTest {
  private String mFilePath;
  private TachyonConf mTachyonConf;

  @Before
  public final void Before() {
    mFilePath = getClass().getClassLoader().getResource("test_metrics.properties").getFile();
    mTachyonConf = new TachyonConf();
    mTachyonConf.set(Constants.METRICS_CONF_FILE, mFilePath);
  }

  @Test
  public void metricsSystemTest() {
    MetricsSystem masterMetricsSystem = new MetricsSystem("master", mTachyonConf);
    masterMetricsSystem.start();

    Assert.assertNotNull(masterMetricsSystem.getServletHandler());
    Assert.assertEquals(2, masterMetricsSystem.getSinks().size());
    Assert.assertEquals(1, masterMetricsSystem.getSources().size());
    masterMetricsSystem.registerSource(new MasterSource(null));
    Assert.assertEquals(2, masterMetricsSystem.getSources().size());
    masterMetricsSystem.stop();

    MetricsSystem workerMetricsSystem = new MetricsSystem("worker", mTachyonConf);
    workerMetricsSystem.start();

    Assert.assertNotNull(workerMetricsSystem.getServletHandler());
    Assert.assertEquals(1, workerMetricsSystem.getSinks().size());
    Assert.assertEquals(1, workerMetricsSystem.getSources().size());
    workerMetricsSystem.registerSource(new WorkerSource(null));
    Assert.assertEquals(2, workerMetricsSystem.getSources().size());
    workerMetricsSystem.stop();
  }
}
