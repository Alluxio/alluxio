package alluxio.kvstore;

import alluxio.collections.Pair;
import alluxio.proto.kvstore.FileCacheStatus;
import alluxio.proto.kvstore.FileCacheStatusKey;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;

import alluxio.proto.kvstore.InodeTreeEdgeKey;
import alluxio.proto.kvstore.InodeTreeEdgeValue;
import com.google.protobuf.InvalidProtocolBufferException;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface KVStoreMetaInterface extends Closeable {
  boolean createFileEntry(FileEntryKey key, FileEntryValue value);

  boolean updateFileEntry(FileEntryKey key, FileEntryValue value);

  boolean updateFileEntryBatch(List<Pair<FileEntryKey, FileEntryValue>> fileEntries);

  Optional<FileEntryValue> getFileEntry(FileEntryKey key);

  boolean deleteFileEntry(FileEntryKey key);

  boolean deleteFileEntryRange(FileEntryKey keyStart, FileEntryKey keyEnd);

  boolean putEntryBatchAtomic(Map<ByteString, ByteString> kvPairs);

  boolean deleteEntryBatchAtomic(List<ByteString> keys);

  List<Pair<FileEntryKey, FileEntryValue>> scanFileEntryKV(FileEntryKey startKey,
      FileEntryKey endKey, int limit);

  List<Pair<FileEntryKey, FileEntryValue>> scanFileEntryKV(FileEntryKey startKey,
      FileEntryKey endKey);

  List<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>> scanEdge(InodeTreeEdgeKey startKey,
      InodeTreeEdgeKey endKey, int limit);

  boolean updateFileCacheStatus(FileCacheStatusKey key, FileCacheStatus value);

  Optional<FileCacheStatus> getFileCacheStatus(FileCacheStatusKey key);

  boolean deleteFileCacheStatus(FileCacheStatusKey key);

  List<Pair<FileCacheStatusKey, FileCacheStatus>> scanFileCacheStatus(FileCacheStatusKey startKey,
      FileCacheStatusKey endKey, int limit);

  Optional<InodeTreeEdgeValue> getInodeTreeEdge(InodeTreeEdgeKey key);

  void deleteInodeTreeEdge(InodeTreeEdgeKey key);

  boolean checkAndRecover();
}
