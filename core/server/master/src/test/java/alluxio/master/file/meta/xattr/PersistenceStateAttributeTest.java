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

package alluxio.master.file.meta.xattr;

import static alluxio.master.file.meta.xattr.ExtendedAttribute.PERSISTENCE_STATE;
import static org.junit.Assert.assertEquals;

import alluxio.master.file.meta.PersistenceState;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PersistenceStateAttributeTest {

  @Test
  public void testSingleEncode() {
    for (int i = 0; i < PersistenceState.values().length; i++) {
      PersistenceState state = PersistenceState.values()[i];
      ArrayList<PersistenceState> a = new ArrayList<>();
      a.add(state);
      assertEquals(a, PERSISTENCE_STATE.decode(
          PERSISTENCE_STATE.encode(a)));
    }
  }

  @Test
  public void testMultiEncode() {
    PersistenceState[] states = { PersistenceState.TO_BE_PERSISTED,
        PersistenceState.TO_BE_PERSISTED, PersistenceState.PERSISTED, PersistenceState.LOST,
        PersistenceState.NOT_PERSISTED, PersistenceState.LOST, PersistenceState.PERSISTED};
    byte[] encoded = PERSISTENCE_STATE.encode(Arrays.asList(states));
    List<PersistenceState> decoded = PERSISTENCE_STATE.decode(encoded);
    for (int i = 0; i < states.length; i++) {
      assertEquals(states[i], decoded.get(i));
    }
  }
}
