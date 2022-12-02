package alluxio.kvstore;

import alluxio.collections.Pair;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.KVStoreTable;

import java.util.List;

class KVStoreTest {
  public static Pair<FileEntryKey, FileEntryValue> createFileEntryKey(long pid, String name, long cid) {
    FileEntryKey key = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(pid)
        .setName(name).build();
    FileEntryValue value = FileEntryValue.newBuilder().setId(cid).build();
    return new Pair<>(key, value);
  }

  public static void printResults(List<Pair<FileEntryKey, FileEntryValue>> results) {
    for (Pair<FileEntryKey, FileEntryValue> p : results) {
      System.out.println(String.format("result : %s----\n%s",
          p.getFirst().toString(),
          p.getSecond().toString()));
    }
  }

  public static void main(String [] args) throws Exception {
    TiKVStoreMetaRaw tiKVStoreMetaRaw = new TiKVStoreMetaRaw();

    Pair<FileEntryKey, FileEntryValue> pair = createFileEntryKey(0, "", 0);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(0, "first", 100);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    boolean result = tiKVStoreMetaRaw.createFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(0, "first_more", 6);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(0, "second", 2);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(0, "third", 3);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(0, "fourth", 4);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(1, "first", 5);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());

    pair = createFileEntryKey(0, "", 1);
    Pair<FileEntryKey, FileEntryValue> pairEnd = createFileEntryKey(1, "", 1);
    List<Pair<FileEntryKey, FileEntryValue>> results = tiKVStoreMetaRaw
        .scanFileEntryKV(pair.getFirst(), pairEnd.getFirst(), 100);
    printResults(results);

    tiKVStoreMetaRaw.close();
  }
}