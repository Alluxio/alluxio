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

package alluxio.util;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public final class JvmPauseMonitorTest {
  @Test
  @Ignore("https://alluxio.atlassian.net/browse/ALLUXIO-3059")
  public void monitorTest() {
    JvmPauseMonitor jvmPauseMonitor = new JvmPauseMonitor();
    jvmPauseMonitor.start();
    List<String> list = Lists.newArrayList();
    int i = 0;
    while (true) {
      list.add(String.valueOf(i++));
      if (jvmPauseMonitor.getWarnTimeExceeded() != 0) {
        jvmPauseMonitor.stop();
        break;
      }
    }
    Assert.assertNotEquals(jvmPauseMonitor.getWarnTimeExceeded(), 0);
    Assert.assertNotEquals(jvmPauseMonitor.getTotalExtraTime(), 0);
  }
}
