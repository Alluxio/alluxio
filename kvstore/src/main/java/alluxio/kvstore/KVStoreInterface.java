package alluxio.kvstore;

import java.io.Closeable;

public interface KVStoreInterface extends Closeable {
  KVStoreMetaInterface getMetaKVStore();

  KVStoreMountInterface getMountKVStore();
}
