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

package alluxio.master.journal.raft;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link OnceSupplier}.
 */
public final class OnceSupplierTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void justOnce() {
    OnceSupplier<Integer> s = new OnceSupplier<>(10);
    assertEquals(10, (long) s.get());
    mThrown.expect(IllegalStateException.class);
    s.get();
  }
}
