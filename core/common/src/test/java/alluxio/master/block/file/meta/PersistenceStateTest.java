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

package alluxio.master.block.file.meta;

import static org.junit.Assert.assertEquals;

import alluxio.master.file.meta.PersistenceState;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PersistenceStateTest {

  @Test
  public void testSingleEncode() {
    for (int i = 0; i < PersistenceState.values().length; i++) {
      PersistenceState state = PersistenceState.values()[i];
      assertEquals(state, PersistenceState.decode(state.encode()));
    }
  }

  @Test
  public void testMultiEncode() {
    PersistenceState[] states = { PersistenceState.TO_BE_PERSISTED,
        PersistenceState.TO_BE_PERSISTED, PersistenceState.PERSISTED, PersistenceState.LOST,
        PersistenceState.NOT_PERSISTED, PersistenceState.LOST, PersistenceState.PERSISTED};
    String encoded = multiEncode(Arrays.asList(states));
    assertEquals(states.length, encoded.length());
    List<PersistenceState> decoded = PersistenceState.decode(encoded);
    for (int i = 0; i < states.length; i++) {
      assertEquals(states[i], decoded.get(i));
    }
  }

  private String multiEncode(List<PersistenceState> states) {
    byte[] encodedStates = new byte[states.size()];
    for (int i = 0; i < states.size(); i++) {
      encodedStates[i] = states.get(i).encode();
    }
    return new String(encodedStates);
  }
}
