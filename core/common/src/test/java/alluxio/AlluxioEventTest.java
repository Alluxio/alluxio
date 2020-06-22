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

package alluxio;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Test for checking consistency of Alluxio events.
 */
public class AlluxioEventTest {

  @Test
  public void testNoDuplicate() {
    Set<Integer> idSet = new HashSet<>();
    for (AlluxioEvent event : AlluxioEvent.values()) {
      if (!idSet.add(event.getId())) {
        Assert.fail(String.format("Duplicate event-id detected: %d.", event.getId()));
      }
    }
  }
}
