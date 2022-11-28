package alluxio.kvstore;

import java.io.IOException;

public class TiKVStore implements KVStoreInterface{
  private KVStoreMountInterface mKVStoreMount;
  private KVStoreMetaInterface mKVStoreMeta;

  public TiKVStore() {
    mKVStoreMeta = new TiKVStoreMetaRaw();
    mKVStoreMount = new TiKVStoreMountRaw();
  }

  @Override
  public KVStoreMetaInterface getMetaKVStore() {
    return mKVStoreMeta;
  }

  @Override
  public KVStoreMountInterface getMountKVStore() {
    return mKVStoreMount;
  }

  @Override
  public void close() throws IOException {
    mKVStoreMount.close();
    mKVStoreMeta.close();
  }
}
