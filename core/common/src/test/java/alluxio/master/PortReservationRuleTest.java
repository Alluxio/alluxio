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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Closeable;

/**
 * Tests for {@link PortReservationRule}.
 */
public final class PortReservationRuleTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public void after() {
    PortRegistry.clear();
  }

  @Test
  public void basic() throws Exception {
    PortReservationRule rule = new PortReservationRule();
    for (int attempt = 0; attempt < 20; attempt++) {
      int port;
      try (Closeable c = rule.toResource()) {
        port = rule.getPort();
        assertFalse("Port should be locked, so we cannot re-lock",
            PortRegistry.INSTANCE.lockPort(port));
      }
      if (PortRegistry.INSTANCE.lockPort(port)) {
        // Successfully locked after closing the rule.
        return;
      }
      // Another process could have taken the port, retry.
    }
    fail("Failed to lock port after closing the port rule");
  }

  @Test
  public void invalidGetPort() {
    PortReservationRule rule = new PortReservationRule();
    mThrown.expect(IllegalStateException.class);
    rule.getPort();
  }
}
