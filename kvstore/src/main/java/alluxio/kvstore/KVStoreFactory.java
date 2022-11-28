package alluxio.kvstore;

public class KVStoreFactory {
  public static KVStoreInterface getKVStoreFactory() {
    return new TiKVStore();
  }
}
