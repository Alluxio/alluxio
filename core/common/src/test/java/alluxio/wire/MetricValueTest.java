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

package alluxio.wire;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class MetricValueTest {

  @Test
  public void json() throws Exception {
    MetricValue metricValue = MetricValue.forLong(new Random().nextLong());
    ObjectMapper mapper = new ObjectMapper();
    MetricValue other =
        mapper.readValue(mapper.writeValueAsBytes(metricValue), MetricValue.class);
    Assert.assertEquals(metricValue.getLongValue(), other.getLongValue());
  }

  @Test
  public void proto() {
    MetricValue metricValue = MetricValue.forLong(new Random().nextLong());
    MetricValue other = MetricValue.fromThrift(metricValue.toThrift());
    Assert.assertEquals(metricValue.getLongValue(), other.getLongValue());
  }
}
