package alluxio.kvstore;

import alluxio.collections.Pair;
import alluxio.proto.kvstore.FileCacheStatus;
import alluxio.proto.kvstore.FileCacheStatusKey;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;

import com.google.protobuf.InvalidProtocolBufferException;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TiKVStoreMetaRaw implements KVStoreMetaInterface, Closeable {
  static final int DEFAULT_TTL = 10;
  private TiSession mTiSession;
  private RawKVClient mRawKVClient;

  public TiKVStoreMetaRaw() {
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
  public boolean createFileEntry(FileEntryKey key, FileEntryValue value) {
    Optional<ByteString> optional
        = Optional.ofNullable(mRawKVClient.putIfAbsent(ByteString.copyFrom(key.toByteArray()),
        ByteString.copyFrom(value.toByteArray())));

    return !optional.isPresent();
  }

  @Override
  public boolean updateFileEntry(FileEntryKey key, FileEntryValue value) {
    mRawKVClient.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(value.toByteArray()));
      return true;
  }

  @Override
  public boolean updateFileEntryBatch(List<Pair<FileEntryKey, FileEntryValue>> fileEntries) {
    Map<ByteString, ByteString> map = new HashMap<>();
    for (Pair<FileEntryKey, FileEntryValue> fileEntry : fileEntries) {
      map.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(fileEntry.getFirst().toByteArray()),
          org.tikv.shade.com.google.protobuf.ByteString.copyFrom(fileEntry.getSecond().toByteArray()));
    }
    mRawKVClient.batchPut(map);
    return true;
  }

  @Override
  public Optional<FileEntryValue> getFileEntry(FileEntryKey key)  {
    try {
      ByteString bytes = mRawKVClient.get(
          org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()));
      if (bytes.size() == 0) {
        return Optional.empty();
      }
      return Optional.of(FileEntryValue.parseFrom(bytes.toByteArray()));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean deleteFileEntry(FileEntryKey key) {
    mRawKVClient.delete(org.tikv.shade.com.google.protobuf.ByteString
        .copyFrom(key.toByteArray()));
    return true;
  }

  @Override
  public boolean deleteFileEntryRange(FileEntryKey keyStart, FileEntryKey keyEnd) {
    mRawKVClient.deleteRange(org.tikv.shade.com.google.protobuf.ByteString
        .copyFrom(keyStart.toByteArray()), org.tikv.shade.com.google.protobuf.ByteString
        .copyFrom(keyEnd.toByteArray()));
    return false;
  }

  @Override
  public boolean putEntryBatchAtomic(Map<ByteString, ByteString> kvPairs) {
    mRawKVClient.batchPut(kvPairs);
    return true;
  }

  @Override
  public boolean deleteEntryBatchAtomic(List<ByteString> keys) {
    mRawKVClient.batchDeleteAtomic(keys);
    return true;
  }

  @Override
  public List<Pair<FileEntryKey, FileEntryValue>> scanFileEntryKV(FileEntryKey startKey,
      FileEntryKey endKey, int limit) {
    try {
      List<org.tikv.kvproto.Kvrpcpb.KvPair> kvPairs = mRawKVClient
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
  }

  @Override
  public List<Pair<FileEntryKey, FileEntryValue>> scanFileEntryKV(FileEntryKey startKey,
      FileEntryKey endKey) {
    try {
      List<org.tikv.kvproto.Kvrpcpb.KvPair> kvPairs = mRawKVClient
          .scan(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(startKey.toByteArray()),
              org.tikv.shade.com.google.protobuf.ByteString.copyFrom(endKey.toByteArray()));

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
  }

  @Override
  public boolean updateFileCacheStatus(FileCacheStatusKey key, FileCacheStatus value) {
    mRawKVClient.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(value.toByteArray()));
    return true;
  }

  @Override
  public Optional<FileCacheStatus> getFileCacheStatus(FileCacheStatusKey key) {
    try {
      ByteString bytes = mRawKVClient.get(
          org.tikv.shade.com.google.protobuf.ByteString.copyFrom(key.toByteArray()));
      if (bytes.size() == 0) {
        return Optional.empty();
      }

      return Optional.of(FileCacheStatus.parseFrom(bytes.toByteArray()));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean deleteFileCacheStatus(FileCacheStatusKey key) {
    mRawKVClient.delete(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
        key.toByteArray()));
    return true;
  }

  @Override
  public List<Pair<FileCacheStatusKey, FileCacheStatus>> scanFileCacheStatus(
      FileCacheStatusKey startKey, FileCacheStatusKey endKey, int limit) {
    try {
      List<org.tikv.kvproto.Kvrpcpb.KvPair> kvPairs = mRawKVClient
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
