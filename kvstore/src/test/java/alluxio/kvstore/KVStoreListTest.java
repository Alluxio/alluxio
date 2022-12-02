package alluxio.kvstore;

import alluxio.collections.Pair;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.InodeTreeEdgeKey;
import alluxio.proto.kvstore.InodeTreeEdgeValue;
import alluxio.proto.kvstore.KVStoreTable;

import javax.crypto.spec.OAEPParameterSpec;
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

    FileEntryKey keyStart = FileEntryKey.newBuilder().setParentID(0)
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setName("").build();
    FileEntryKey keyEnd = FileEntryKey.newBuilder()
        .setParentID(0XFFFFFFFF)
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setName("").build();
    List<Pair<FileEntryKey, FileEntryValue>> list = tiKVStoreMetaRaw.scanFileEntryKV(keyStart, keyEnd);
    printFileEntryResults(list);

    InodeTreeEdgeKey inodeTreeEdgeKeyStart = InodeTreeEdgeKey.newBuilder()
        .setTableType(KVStoreTable.INODE_EDGE)
        .setId(0)
        .build();
    InodeTreeEdgeKey inodeTreeEdgeKeyEnd = InodeTreeEdgeKey.newBuilder()
        .setTableType(KVStoreTable.INODE_EDGE)
        .setId(0XFFFFFFFF)
        .build();
    List<Pair<InodeTreeEdgeKey, InodeTreeEdgeValue>> listEdge
        = tiKVStoreMetaRaw.scanEdge(inodeTreeEdgeKeyStart, inodeTreeEdgeKeyEnd, 1000);
    printResults(listEdge);

    Optional<FileEntryValue> entryValue = tiKVStoreMetaRaw.getFileEntry(keyEnd);
    tiKVStoreMetaRaw.close();
  }
}