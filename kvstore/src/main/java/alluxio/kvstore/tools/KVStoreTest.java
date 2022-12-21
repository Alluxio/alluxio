package alluxio.kvstore.tools;

import alluxio.collections.Pair;
import alluxio.kvstore.TiKVStoreMetaRaw;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.KVEntryType;
import alluxio.proto.kvstore.KVStoreTable;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class KVStoreTest {
  public static Pair<FileEntryKey, FileEntryValue> createFileEntryKey(long pid, String name, long cid) {
    FileEntryKey key = FileEntryKey.newBuilder()
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setParentID(pid)
        .setName(name).build();
    FileEntryValue value = FileEntryValue.newBuilder().setId(cid)
        .setEntryType(KVEntryType.FILE).build();
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

    Pair<FileEntryKey, FileEntryValue> pair = createFileEntryKey(11, "", 0);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(11, "first", 100);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    boolean result = tiKVStoreMetaRaw.createFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(11, "first_more", 6);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(11, "second", 2);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(11, "third", 3);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(11, "fourth", 4);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());
    pair = createFileEntryKey(12, "first", 5);
    tiKVStoreMetaRaw.updateFileEntry(pair.getFirst(), pair.getSecond());

    pair = createFileEntryKey(1, "", 1);
    Pair<FileEntryKey, FileEntryValue> pairEnd = createFileEntryKey(0xEFFFFFFF, "", 1);
    List<Pair<FileEntryKey, FileEntryValue>> results = tiKVStoreMetaRaw
        .scanFileEntryKV(pair.getFirst(), pairEnd.getFirst(), 100);
    printResults(results);

    System.out.println("Begin to put batch");
    Map<ByteString, ByteString> map = new HashMap<ByteString, ByteString>();
    pair = createFileEntryKey(100, "first_batch", 15);
    map.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
        pair.getFirst().toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
            pair.getSecond().toByteArray()));
    pair = createFileEntryKey(100, "second_batch", 16);
    map.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
            pair.getFirst().toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
            pair.getSecond().toByteArray()));
    tiKVStoreMetaRaw.putEntryBatchAtomic(map);

    System.out.println("pid 100 done");
    map.clear();
    pair = createFileEntryKey(1000, "first_batch1", 150);
    map.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
            pair.getFirst().toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
            pair.getSecond().toByteArray()));
    pair = createFileEntryKey(800000, "second_batch2", 1600);
    map.put(org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
            pair.getFirst().toByteArray()),
        org.tikv.shade.com.google.protobuf.ByteString.copyFrom(
            pair.getSecond().toByteArray()));
    tiKVStoreMetaRaw.putEntryBatchAtomic(map);

    System.out.println("pid 1000 and 10000 done");
    results = tiKVStoreMetaRaw
        .scanFileEntryKV(pair.getFirst(), pairEnd.getFirst(), 100);
    printResults(results);

    tiKVStoreMetaRaw.close();
  }
}