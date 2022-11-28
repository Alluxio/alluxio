package alluxio.kvstore;

import alluxio.collections.Pair;
import alluxio.proto.kvstore.MountEntryValue;
import alluxio.proto.kvstore.MountTableKey;
import com.google.protobuf.InvalidProtocolBufferException;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class TiKVStoreMountRaw implements KVStoreMountInterface, Closeable {
  static final int DEFAULT_TTL = 10;
  private TiSession mTiSession;
  private RawKVClient mRawKVClient;

  public TiKVStoreMountRaw() {
    String pdAddrsStr = getEnv("TXNKV_PD_ADDRESSES");
    pdAddrsStr = pdAddrsStr == null ? "localhost:2379" : pdAddrsStr;
    TiConfiguration conf = TiConfiguration.createRawDefault(pdAddrsStr);
    mTiSession = TiSession.create(conf);
    mRawKVClient = mTiSession.createRawClient();
  }

  protected static String getEnv(String key) {
    String tmp = System.getenv(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    tmp = System.getProperty(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    return null;
  }

  @Override
  public boolean createMountEntry(MountTableKey key, MountEntryValue value) {
    Optional<ByteString> optional
        = Optional.ofNullable(mRawKVClient.putIfAbsent(ByteString.copyFrom(key.toByteArray()),
        ByteString.copyFrom(value.toByteArray())));

    return !optional.isPresent();
  }

  @Override
  public boolean updateMountEntry(MountTableKey key, MountEntryValue value) {
    mRawKVClient.put(ByteString.copyFrom(key.toByteArray()),
        ByteString.copyFrom(value.toByteArray()));
    return true;
  }

  @Override
  public MountEntryValue getMountEntry(MountTableKey key) throws InvalidProtocolBufferException {
    return MountEntryValue.parseFrom(
        mRawKVClient.get(
                ByteString.copyFrom(key.toByteArray()))
            .toByteArray());
  }

  @Override
  public boolean deleteMountEntry(MountTableKey key) {
    mRawKVClient.delete(ByteString.copyFrom(
        key.toByteArray()));
    return true;
  }

  @Override
  public List<Pair<MountTableKey, MountEntryValue>> scanMountEntryKV(MountTableKey startKey,
      MountTableKey endKey, int limit) throws InvalidProtocolBufferException {
    List<Kvrpcpb.KvPair> kvPairs = mRawKVClient
        .scan(ByteString.copyFrom(startKey.toByteArray()),
            ByteString.copyFrom(endKey.toByteArray()), limit);

    List<Pair<MountTableKey, MountEntryValue>> list
        = new LinkedList<Pair<MountTableKey, MountEntryValue>>();
    for (Kvrpcpb.KvPair kvPair : kvPairs) {
      list.add(new Pair<>(MountTableKey.parseFrom(kvPair.getKey().toByteArray()),
          MountEntryValue.parseFrom(kvPair.getValue().toByteArray())));
    }

    return list;
  }

  @Override
  public boolean checkAndRecover() {
    return false;
  }

  @Override
  public void close() {
    mRawKVClient.close();
    try {
      mTiSession.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
