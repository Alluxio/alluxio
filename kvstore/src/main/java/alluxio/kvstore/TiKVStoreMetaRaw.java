package alluxio.kvstore;

import alluxio.collections.Pair;
import alluxio.proto.kvstore.FileCacheStatus;
import alluxio.proto.kvstore.FileCacheStatusKey;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.InodeTreeEdgeKey;
import alluxio.proto.kvstore.InodeTreeEdgeValue;
import alluxio.resource.ResourcePool;

import com.google.protobuf.InvalidProtocolBufferException;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TiKVStoreMetaRaw implements KVStoreMetaInterface, Closeable {
  static final int DEFAULT_TTL = 10;
  private TiSession mTiSession;
  private ResourcePool<RawKVClient> mRawKVClientPool;

  public TiKVStoreMetaRaw() {
    String pdAddrsStr = getEnv("TXNKV_PD_ADDRESSES");
    pdAddrsStr = pdAddrsStr == null ? "localhost:2379" : pdAddrsStr;
    TiConfiguration conf = TiConfiguration.createRawDefault(pdAddrsStr);
    // conf.setEnableAtomicForCAS(true);
    mTiSession = TiSession.create(conf);
    mRawKVClientPool = new ResourcePool<RawKVClient>(64) {
      @Override
      public void close() {
        for (RawKVClient rawKVClient : mResources) {
          rawKVClient.close();
        }
      }

      @Override
      public RawKVClient createNewResource() {
        return mTiSession.createRawClient();
      }
    };
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
  public boolean createFileEntry(FileEntryKey key, FileEntryValue value) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      // Optional<ByteString> optional
      //    = rawKVClient.putIfAbsent(ByteString.copyFrom(key.toByteArray()),
      //    ByteString.copyFrom(value.toByteArray()));
      // return !optional.isPresent();
      rawKVClient.put(ByteString.copyFrom(key.toByteArray()),
          ByteString.copyFrom(value.toByteArray()));
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean updateFileEntry(FileEntryKey key, FileEntryValue value) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(value.toByteArray()));
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean updateFileEntryBatch(List<Pair<FileEntryKey, FileEntryValue>> fileEntries) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      Map<ByteString, ByteString> map = new HashMap<>();
      for (Pair<FileEntryKey, FileEntryValue> fileEntry : fileEntries) {
        map.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(fileEntry.getFirst().toByteArray()),
            org.tikv.shade.com.google.protobuf.ByteString.copyFrom(fileEntry.getSecond().toByteArray()));
      }
      rawKVClient.batchPut(map);
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public Optional<FileEntryValue> getFileEntry(FileEntryKey key)  {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      try {
        Optional<ByteString> bytes = rawKVClient.get(
            org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()));
        if (!bytes.isPresent()) {
          return Optional.empty();
        }
        return Optional.of(FileEntryValue.parseFrom(bytes.get().toByteArray()));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean deleteFileEntry(FileEntryKey key) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.delete(org.tikv.shade.com.google.protobuf.ByteString
          .copyFrom(key.toByteArray()));
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean deleteFileEntryRange(FileEntryKey keyStart, FileEntryKey keyEnd) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.deleteRange(org.tikv.shade.com.google.protobuf.ByteString
          .copyFrom(keyStart.toByteArray()), org.tikv.shade.com.google.protobuf.ByteString
          .copyFrom(keyEnd.toByteArray()));
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean putEntryBatchAtomic(Map<ByteString, ByteString> kvPairs) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.batchPut(kvPairs);
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean deleteEntryBatchAtomic(List<ByteString> keys) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.batchDelete(keys);
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public List<Pair<FileEntryKey, FileEntryValue>> scanFileEntryKV(FileEntryKey startKey,
      FileEntryKey endKey, int limit) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      try {
        List<org.tikv.kvproto.Kvrpcpb.KvPair> kvPairs = rawKVClient
            .scan(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(startKey.toByteArray()),
                org.tikv.shade.com.google.protobuf.ByteString.copyFrom(endKey.toByteArray()), limit);

        List<Pair<FileEntryKey, FileEntryValue>> list
            = new LinkedList<Pair<FileEntryKey, FileEntryValue>>();
        for (Kvrpcpb.KvPair kvPair : kvPairs) {
          list.add(new Pair<>(FileEntryKey.parseFrom(kvPair.getKey().toByteArray()), FileEntryValue
              .parseFrom(kvPair.getValue().toByteArray())));
        }

        return list;
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public List<Pair<FileEntryKey, FileEntryValue>> scanFileEntryKV(FileEntryKey startKey) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      try {
        List<org.tikv.kvproto.Kvrpcpb.KvPair> kvPairs = rawKVClient
            .scanPrefix(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(startKey.toByteArray()));

        List<Pair<FileEntryKey, FileEntryValue>> list
            = new LinkedList<Pair<FileEntryKey, FileEntryValue>>();
        for (Kvrpcpb.KvPair kvPair : kvPairs) {
          list.add(new Pair<>(FileEntryKey.parseFrom(kvPair.getKey().toByteArray()), FileEntryValue
              .parseFrom(kvPair.getValue().toByteArray())));
        }

        return list;
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean updateFileCacheStatus(FileCacheStatusKey key, FileCacheStatus value) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()),
          org.tikv.shade.com.google.protobuf.ByteString.copyFrom(value.toByteArray()));
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public Optional<FileCacheStatus> getFileCacheStatus(FileCacheStatusKey key) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      try {
        Optional<ByteString> bytes = rawKVClient.get(
            org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()));
        if (!bytes.isPresent()) {
          return Optional.empty();
        }

        return Optional.of(FileCacheStatus.parseFrom(bytes.get().toByteArray()));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean deleteFileCacheStatus(FileCacheStatusKey key) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.delete(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
          key.toByteArray()));
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public List<Pair<FileCacheStatusKey, FileCacheStatus>> scanFileCacheStatus(
      FileCacheStatusKey startKey, FileCacheStatusKey endKey, int limit) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      try {
        List<org.tikv.kvproto.Kvrpcpb.KvPair> kvPairs = rawKVClient
            .scan(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(startKey.toByteArray()),
                org.tikv.shade.com.google.protobuf.ByteString.copyFrom(endKey.toByteArray()), limit);

        List<Pair<FileCacheStatusKey, FileCacheStatus>> list
            = new LinkedList<Pair<FileCacheStatusKey, FileCacheStatus>>();
        for (Kvrpcpb.KvPair kvPair : kvPairs) {
          list.add(new Pair<>(FileCacheStatusKey.parseFrom(kvPair.getKey().toByteArray()),
              FileCacheStatus.parseFrom(kvPair.getValue().toByteArray())));
        }

        return list;
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public Optional<InodeTreeEdgeValue> getInodeTreeEdge(InodeTreeEdgeKey key) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      try {
        Optional<ByteString> bytes = rawKVClient.get(
            org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()));
        if (!bytes.isPresent()) {
          return Optional.empty();
        }

        return Optional.of(InodeTreeEdgeValue.parseFrom(bytes.get().toByteArray()));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public void updateInodeTreeEdge(InodeTreeEdgeKey key, InodeTreeEdgeValue value) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()),
          org.tikv.shade.com.google.protobuf.ByteString.copyFrom(value.toByteArray()));
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public void deleteInodeTreeEdge(InodeTreeEdgeKey key) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.delete(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
          key.toByteArray()));
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean deleteInodeTreeEdge(InodeTreeEdgeKey keyStart, InodeTreeEdgeKey keyEnd) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      rawKVClient.deleteRange(org.tikv.shade.com.google.protobuf.ByteString
          .copyFrom(keyStart.toByteArray()), org.tikv.shade.com.google.protobuf.ByteString
          .copyFrom(keyEnd.toByteArray()));
      return true;
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public List<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>> scanEdge(InodeTreeEdgeKey startKey,
      InodeTreeEdgeKey endKey, int limit) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      try {
        List<org.tikv.kvproto.Kvrpcpb.KvPair> kvPairs = rawKVClient
            .scan(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(startKey.toByteArray()),
                org.tikv.shade.com.google.protobuf.ByteString.copyFrom(endKey.toByteArray()), limit);

        List<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>> list
            = new LinkedList<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>>();
        for (Kvrpcpb.KvPair kvPair : kvPairs) {
          list.add(new Pair<>(InodeTreeEdgeKey.parseFrom(kvPair.getKey().toByteArray()),
              InodeTreeEdgeValue.parseFrom(kvPair.getValue().toByteArray())));
        }

        return list;
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public List<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>> scanEdgePrefix(InodeTreeEdgeKey prefixKey) {
    RawKVClient rawKVClient = mRawKVClientPool.acquire();
    try {
      try {
        List<org.tikv.kvproto.Kvrpcpb.KvPair> kvPairs = rawKVClient
            .scanPrefix(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(prefixKey.toByteArray()));

        List<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>> list
            = new LinkedList<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>>();
        for (Kvrpcpb.KvPair kvPair : kvPairs) {
          list.add(new Pair<>(InodeTreeEdgeKey.parseFrom(kvPair.getKey().toByteArray()),
              InodeTreeEdgeValue.parseFrom(kvPair.getValue().toByteArray())));
        }

        return list;
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } finally {
      mRawKVClientPool.release(rawKVClient);
    }
  }

  @Override
  public boolean checkAndRecover() {
    return false;
  }

  @Override
  public void close() {
    try {
        mRawKVClientPool.close();
      } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      mTiSession.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
