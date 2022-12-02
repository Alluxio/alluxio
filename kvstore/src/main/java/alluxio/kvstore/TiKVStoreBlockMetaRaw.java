package alluxio.kvstore;

import alluxio.proto.kvstore.BlockLocationKey;
import alluxio.proto.kvstore.BlockLocationValue;
import alluxio.proto.kvstore.KVStoreTable;
import alluxio.resource.CloseableIterator;

import com.google.protobuf.InvalidProtocolBufferException;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.Iterator;
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
    ByteString bytes = mRawKVClient.get(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(id.toByteArray()));
    if (bytes.size() == 0) {
      return Optional.empty();
    }

    try {
      return Optional.of(BlockLocationValue.parseFrom(bytes.toByteArray()));
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

  }

  @Override
  public List<BlockLocationValue> getLocations(BlockLocationKey id) {
    List<BlockLocationValue> blockLocationValues = new LinkedList<>();
    BlockLocationKey endId = BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION)
        .setFileId(id.getFileId() + 1)
        .build();
    List<org.tikv.kvproto.Kvrpcpb.KvPair> results = mRawKVClient.scan(
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(id.toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(endId.toByteArray()));
    try {
      for (org.tikv.kvproto.Kvrpcpb.KvPair kvPair : results) {
        blockLocationValues.add(BlockLocationValue.parseFrom(kvPair.getValue().toByteArray()));
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

  /*
  @Override
  public CloseableIterator<Block> getCloseableIterator() {
    Iterator<Block> iter = new Iterator<Block>() {
      BlockLocationKey mStartKey = BlockLocationKey.newBuilder()
          .setTableType(KVStoreTable.BLOCK_LOCATION)
          .setFileId(0)
          .build();
      Iterator<Block> mIter;
      boolean mFinished = false;

      @Override
      public boolean hasNext() {
        if (mFinished) {
          return false;
        }

        if (mIter.hasNext()) {
          return true;
        }

        List<org.tikv.kvproto.Kvrpcpb.KvPair> results = mRawKVClient
            .scan(org.tikv.shade.com.google.protobuf.ByteString.copyFrom((mStartKey.toByteArray())),
                1000);
        List<Block> blocks = new LinkedList<>();
        BlockLocationKey blockLocationKey = null;
        try {
          for (Kvrpcpb.KvPair kvPair : results) {
            blockLocationKey = BlockLocationKey.parseFrom(kvPair.getKey().toByteArray());
            BlockLocationValue blockLocationValue = BlockLocationValue.parseFrom(kvPair.getValue().toByteArray());
            blocks.add(new Block(blockLocationKey, blockLocationValue));
          }
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }

        if (blocks.isEmpty()) {
          mFinished = true;
          return false;
        }

        if (blockLocationKey != null && blockLocationKey.hasWorkerId()) {
          mStartKey = BlockLocationKey.newBuilder()
              .setTableType(KVStoreTable.BLOCK_LOCATION)
              .setFileId(blockLocationKey.getFileId())
              .setWorkerId(blockLocationKey.getWorkerId() + 1)
              .build();
        } else {
          mStartKey = BlockLocationKey.newBuilder()
              .setTableType(KVStoreTable.BLOCK_LOCATION)
              .setFileId(blockLocationKey.getFileId() + 1)
              .build();
        }

        mIter = blocks.stream().iterator();
        return true;
      }

      @Override
      public Block next() {
        return mIter.next();
      }
    };

    return new CloseableIterator<Block>(iter) {
      @Override
      public void closeResource() {
      }
    };
  }
   */
}
