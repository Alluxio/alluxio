package alluxio.kvstore.tools;

import alluxio.collections.Pair;
import alluxio.kvstore.TiKVStoreBlockMetaRaw;
import alluxio.proto.kvstore.BlockLocationKey;
import alluxio.proto.kvstore.BlockLocationValue;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.KVStoreTable;
import com.google.protobuf.InvalidProtocolBufferException;
import org.tikv.kvproto.Kvrpcpb;

import java.util.List;

class KVStoreBlockClearTest {
  public static Pair<FileEntryKey, FileEntryValue> createFileEntryKey(long pid, String name, long cid) {
    FileEntryKey key = FileEntryKey.newBuilder().setParentID(pid)
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setName(name).build();
    FileEntryValue value = FileEntryValue.newBuilder().setId(cid).build();
    return new Pair<>(key, value);
  }

  public static void printResults(List<Kvrpcpb.KvPair> results)
      throws InvalidProtocolBufferException {
    for (Kvrpcpb.KvPair pair : results) {
      System.out.println(String.format("result : %s----\n%s",
          BlockLocationKey.parseFrom(pair.getKey().toByteArray()).toString(),
          BlockLocationValue.parseFrom(pair.getValue().toByteArray()).toString()));
    }
  }

  public static void main(String [] args) throws Exception {
    TiKVStoreBlockMetaRaw tiKVStoreBlockMetaRaw = new TiKVStoreBlockMetaRaw();

    tiKVStoreBlockMetaRaw.clear();

    tiKVStoreBlockMetaRaw.close();
  }
}