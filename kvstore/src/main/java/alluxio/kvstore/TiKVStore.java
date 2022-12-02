package alluxio.kvstore;

import java.io.IOException;

public class TiKVStore implements KVStoreInterface{
  private KVStoreMountInterface mKVStoreMount;
  private KVStoreMetaInterface mKVStoreMeta;
  private KVStoreBlockMeta mKVStoreBlock;

  public TiKVStore() {
    mKVStoreMeta = new TiKVStoreMetaRaw();
    mKVStoreMount = new TiKVStoreMountRaw();
    mKVStoreBlock = new TiKVStoreBlockMetaRaw();
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
  public KVStoreBlockMeta getBlockKVStore() {
    return mKVStoreBlock;
  }

  @Override
  public void close() throws IOException {
    mKVStoreMount.close();
    mKVStoreMeta.close();
    mKVStoreBlock.close();
  }
}
