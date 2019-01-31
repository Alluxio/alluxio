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

package alluxio.master;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link PortRegistry}.
 */
public final class PortRegistryTest {
  private PortRegistry mRegistry = new PortRegistry();

  @Test
  public void lockOnce() {
    int port = PortRegistry.getFreePort();
    boolean locked = false;
    for (int i = 0; i < 100; i++) {
      if (mRegistry.lockPort(port)) {
        locked = true;
        break;
      }
    }
    assertTrue(locked);
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse(mRegistry.lockPort(port));
    }
  }

  @Test
  public void lockMany() {
    int numPorts = 100;
    Set<Integer> ports = new HashSet<>();
    for (int i = 0; i < numPorts; i++) {
      ports.add(mRegistry.reservePort());
    }
    assertEquals(numPorts, ports.size());
  }

  @Test
  public void lockAndRelease() {
    int port = PortRegistry.getFreePort();
    int successes = 0;
    for (int i = 0; i < 100; i++) {
      if (mRegistry.lockPort(port)) {
        successes++;
        mRegistry.release(port);
      }
    }
    // Other processes could interfere and steal the lock occasionally, so we only check > 50.
    assertThat(successes, greaterThan(50));
  }
}
