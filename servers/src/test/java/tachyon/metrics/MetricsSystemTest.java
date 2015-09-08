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

import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.conf.TachyonConf;
import tachyon.master.MasterSource;
import tachyon.worker.WorkerSource;

public class MetricsSystemTest {
  private MetricsConfig mMetricsConfig;
  private TachyonConf mTachyonConf;

  @Before
  public final void Before() {
    mTachyonConf = new TachyonConf();
    Properties metricsProps = new Properties();
    metricsProps.setProperty("*.sink.console.class", "tachyon.metrics.sink.ConsoleSink");
    metricsProps.setProperty("*.sink.console.period", "15");
    metricsProps.setProperty("*.source.jvm.class", "tachyon.metrics.source.JvmSource");
    metricsProps.setProperty("master.sink.console.period", "20");
    metricsProps.setProperty("master.sink.console.unit", "minutes");
    metricsProps.setProperty("master.sink.jmx.class", "tachyon.metrics.sink.JmxSink");
    mMetricsConfig = new MetricsConfig(metricsProps);
  }

  @Test
  public void metricsSystemTest() {
    class Tester implements MetricsSystemTester {
      MetricsSystem.PrivateAccess mPrivateAccess;

      public void receiveAccess(MetricsSystem.PrivateAccess access) {
        mPrivateAccess = access;
      }
    }

    MetricsSystem masterMetricsSystem = new MetricsSystem("master", mMetricsConfig, mTachyonConf);
    Tester masterTester = new Tester();
    masterMetricsSystem.grantAccess(masterTester);
    masterMetricsSystem.start();

    Assert.assertNotNull(masterMetricsSystem.getServletHandler());
    Assert.assertEquals(2, masterTester.mPrivateAccess.getSinks().size());
    Assert.assertEquals(1, masterTester.mPrivateAccess.getSources().size());
    masterMetricsSystem.registerSource(new MasterSource(null));
    Assert.assertEquals(2, masterTester.mPrivateAccess.getSources().size());
    masterMetricsSystem.stop();

    MetricsSystem workerMetricsSystem = new MetricsSystem("worker", mMetricsConfig, mTachyonConf);
    Tester workerTester = new Tester();
    workerMetricsSystem.grantAccess(workerTester);
    workerMetricsSystem.start();

    Assert.assertNotNull(workerMetricsSystem.getServletHandler());
    Assert.assertEquals(1, workerTester.mPrivateAccess.getSinks().size());
    Assert.assertEquals(1, workerTester.mPrivateAccess.getSources().size());
    workerMetricsSystem.registerSource(new WorkerSource());
    Assert.assertEquals(2, workerTester.mPrivateAccess.getSources().size());
    workerMetricsSystem.stop();
  }
}
