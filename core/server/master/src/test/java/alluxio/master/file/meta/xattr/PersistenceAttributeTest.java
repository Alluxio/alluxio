package alluxio.master.file.meta.xattr;

import static alluxio.master.file.meta.xattr.ExtendedAttribute.PERSISTENCE_STATE;
import static org.junit.Assert.assertEquals;

import alluxio.master.file.meta.PersistenceState;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PersistenceAttributeTest {

  @Test
  public void testSingleEncode() throws Exception {
    for (int i = 0; i < PersistenceState.values().length; i++) {
      PersistenceState state = PersistenceState.values()[i];
      assertEquals(state, PERSISTENCE_STATE.decode(
          PERSISTENCE_STATE.encode(state)));
    }
  }

  @Test
  public void testMultiEncode() throws Exception {
    PersistenceState[] states = { PersistenceState.TO_BE_PERSISTED,
        PersistenceState.TO_BE_PERSISTED, PersistenceState.PERSISTED, PersistenceState.LOST,
        PersistenceState.NOT_PERSISTED, PersistenceState.LOST, PersistenceState.PERSISTED};
    ByteString encoded = PERSISTENCE_STATE.multiEncode(Arrays.asList(states));
    assertEquals(states.length, encoded.size() / PERSISTENCE_STATE.getEncodedSize());
    List<PersistenceState> decoded = PERSISTENCE_STATE.multiDecode(encoded);
    for (int i = 0; i < states.length; i++) {
      assertEquals(states[i], decoded.get(i));
    }
  }
}
