package alluxio.kvstore.tools;

import alluxio.collections.Pair;
import alluxio.kvstore.TiKVStoreMetaRaw;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.InodeTreeEdgeKey;
import alluxio.proto.kvstore.InodeTreeEdgeValue;
import alluxio.proto.kvstore.KVStoreTable;

import java.util.List;
import java.util.Optional;

class KVStoreListTest {
  public static Pair<FileEntryKey, FileEntryValue> createFileEntryKey(long pid, String name, long cid) {
    FileEntryKey key = FileEntryKey.newBuilder().setParentID(pid)
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setName(name).build();
    FileEntryValue value = FileEntryValue.newBuilder().setId(cid).build();
    return new Pair<>(key, value);
  }

  public static void printFileEntryResults(List<Pair<FileEntryKey, FileEntryValue>> results) {
    for (Pair<FileEntryKey, FileEntryValue> p : results) {
      System.out.println(String.format("result : %s----\n%s",
          p.getFirst().toString(),
          p.getSecond().toString()));
    }
  }

  public static void printResults(List<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>> results) {
    for (Pair<InodeTreeEdgeKey, InodeTreeEdgeValue> p : results) {
      System.out.println(String.format("result : %s----\n%s",
          p.getFirst().toString(),
          p.getSecond().toString()));
    }
  }

  public static void main(String [] args) throws Exception {
    TiKVStoreMetaRaw tiKVStoreMetaRaw = new TiKVStoreMetaRaw();

    FileEntryKey keyStart = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY).build();
    FileEntryKey keyEnd = FileEntryKey.newBuilder()
        .setParentID(0XFFFFFFFF)
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setName("").build();
    List<Pair<FileEntryKey, FileEntryValue>> list = tiKVStoreMetaRaw.scanFileEntryKV(keyStart);
    printFileEntryResults(list);

    InodeTreeEdgeKey inodeTreeEdgeKeyStart = InodeTreeEdgeKey.newBuilder()
        .setTableType(KVStoreTable.INODE_EDGE)
        .build();
    List<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>> listEdge
        = tiKVStoreMetaRaw.scanEdgePrefix(inodeTreeEdgeKeyStart);
    printResults(listEdge);

    Optional<FileEntryValue> entryValue = tiKVStoreMetaRaw.getFileEntry(keyEnd);
    tiKVStoreMetaRaw.close();
  }
}