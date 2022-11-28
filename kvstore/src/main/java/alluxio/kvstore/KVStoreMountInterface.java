package alluxio.kvstore;

import alluxio.collections.Pair;
import alluxio.proto.kvstore.MountEntryValue;
import alluxio.proto.kvstore.MountTableKey;

import com.google.protobuf.InvalidProtocolBufferException;

import java.io.Closeable;
import java.util.List;

public interface KVStoreMountInterface extends Closeable {
  boolean createMountEntry(MountTableKey key, MountEntryValue value);

  public boolean updateMountEntry(MountTableKey key, MountEntryValue value);

  MountEntryValue getMountEntry(MountTableKey key) throws InvalidProtocolBufferException;

  boolean deleteMountEntry(MountTableKey key);

  List<Pair<MountTableKey, MountEntryValue>> scanMountEntryKV(MountTableKey startKey,
      MountTableKey endKey, int limit) throws InvalidProtocolBufferException;

  boolean checkAndRecover();
}
