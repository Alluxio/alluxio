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

package alluxio.stress.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class IOTaskResultTest {
  @Test
  public void json() throws Exception {
    IOTaskResult result = new IOTaskResult();
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.READ, 100L, 20));
    result.addPoint(new IOTaskResult.Point(IOTaskResult.IOMode.WRITE, 100L, 5));
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(result);
    IOTaskResult other = mapper.readValue(json, IOTaskResult.class);
    checkEquality(result, other);
  }

  private void checkEquality(IOTaskResult a, IOTaskResult b) {
    assertEquals(a.getPoints().size(), b.getPoints().size());
    Set<IOTaskResult.Point> points = new HashSet<>(a.getPoints());
    for (IOTaskResult.Point p : b.getPoints()) {
      assertTrue(points.contains(p));
    }
    assertEquals(a.getErrors().size(), b.getErrors().size());
    Set<String> errors = new HashSet<>(a.getErrors());
    for (String e : b.getErrors()) {
      assertTrue(errors.contains(e));
    }
  }
}
