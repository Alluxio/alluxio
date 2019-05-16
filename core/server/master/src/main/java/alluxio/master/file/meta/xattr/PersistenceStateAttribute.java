package alluxio.master.file.meta.xattr;

import alluxio.master.file.meta.PersistenceState;

import com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * An implementation of an extended attribute for {@link PersistenceState}.
 */
public class PersistenceStateAttribute extends AbstractExtendedAttribute<PersistenceState> {

  PersistenceStateAttribute() {
    super(NamespacePrefix.SYSTEM, "ps",
        (int) Math.ceil(Math.log((double) PersistenceState.values().length) / 8));
  }

  @Override
  public ByteString encode(PersistenceState state) {
    return ByteString.copyFrom(new byte[]{(byte) state.ordinal()});
  }

  @Override
  public PersistenceState decode(ByteString bytes) throws IOException {
    if (bytes.size() > 1) {
      throw new IOException("Unable to convert bytes to persistenceState");
    }
    int loc = bytes.byteAt(0) & 0xFF;
    return PersistenceState.values()[loc];
  }
}
