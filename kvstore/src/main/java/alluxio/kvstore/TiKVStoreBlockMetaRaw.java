package alluxio.kvstore;

import alluxio.proto.kvstore.BlockLocationKey;
import alluxio.proto.kvstore.BlockLocationValue;
import alluxio.proto.kvstore.KVStoreTable;

import com.google.protobuf.InvalidProtocolBufferException;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class TiKVStoreBlockMetaRaw implements KVStoreBlockMeta {
  private TiSession mTiSession;
  private RawKVClient mRawKVClient;

  public TiKVStoreBlockMetaRaw() {
    String pdAddrsStr = getEnv("TXNKV_PD_ADDRESSES");
    pdAddrsStr = pdAddrsStr == null ? "localhost:2379" : pdAddrsStr;
    TiConfiguration conf = TiConfiguration.createRawDefault(pdAddrsStr);
    conf.setEnableAtomicForCAS(true);
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
  public Optional<BlockLocationValue> getBlock(BlockLocationKey id) {
    Optional<ByteString> bytes
        = mRawKVClient.get(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(id.toByteArray()));
    if (!bytes.isPresent()) {
      return Optional.empty();
    }

    try {
      return Optional.of(BlockLocationValue.parseFrom(bytes.get().toByteArray()));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putBlock(BlockLocationKey id, BlockLocationValue block) {
    mRawKVClient.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(id.toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(block.toByteArray()));
  }

  @Override
  public void removeBlock(BlockLocationKey id) {
    mRawKVClient.delete(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(id.toByteArray()));
  }

  @Override
  public void clear() {
    BlockLocationKey startKey = BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION)
        .setFileId(0)
        .build();
    BlockLocationKey endKey = BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION)
        .setFileId(0xFFFFFFFF)
        .build();
    mRawKVClient.deleteRange(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(startKey.toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(endKey.toByteArray()));
  }

  @Override
  public List<BlockLocationValue> getLocations(BlockLocationKey id) {
    List<BlockLocationValue> blockLocationValues = new LinkedList<>();
    List<org.tikv.kvproto.Kvrpcpb.KvPair> results = mRawKVClient.scanPrefix(
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(id.toByteArray()));
    try {
      for (org.tikv.kvproto.Kvrpcpb.KvPair kvPair : results) {
        BlockLocationKey key = BlockLocationKey.parseFrom(kvPair.getKey().toByteArray());
        if (key.hasWorkerId()) {
          blockLocationValues.add(BlockLocationValue.parseFrom(kvPair.getValue().toByteArray()));
        }
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    return blockLocationValues;
  }

  @Override
  public void addLocation(BlockLocationKey id, BlockLocationValue location) {
    mRawKVClient.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(id.toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(location.toByteArray()));
  }

  @Override
  public void removeLocation(BlockLocationKey blockId) {
    mRawKVClient.delete(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(blockId.toByteArray()));
  }

  @Override
  public void close() {
    mRawKVClient.close();
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public List<org.tikv.kvproto.Kvrpcpb.KvPair> scan(
      byte [] keyStart, int limit) {
    return mRawKVClient.scan(
            org.tikv.shade.com.google.protobuf.ByteString.copyFrom(keyStart),
            limit);
  }
}
