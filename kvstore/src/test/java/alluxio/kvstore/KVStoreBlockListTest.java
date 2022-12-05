package alluxio.kvstore;

import alluxio.collections.Pair;
import alluxio.proto.kvstore.BlockLocationKey;
import alluxio.proto.kvstore.BlockLocationValue;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.kvstore.InodeTreeEdgeKey;
import alluxio.proto.kvstore.InodeTreeEdgeValue;
import alluxio.proto.kvstore.KVStoreTable;
import com.google.protobuf.InvalidProtocolBufferException;
import org.tikv.kvproto.Kvrpcpb;

import java.util.List;
import java.util.Optional;

class KVStoreBlockListTest {
  public static Pair<FileEntryKey, FileEntryValue> createFileEntryKey(long pid, String name, long cid) {
    FileEntryKey key = FileEntryKey.newBuilder().setParentID(pid)
        .setTableType(KVStoreTable.FILE_ENTRY)
        .setName(name).build();
    FileEntryValue value = FileEntryValue.newBuilder().setId(cid).build();
    return new Pair<>(key, value);
  }

  public static void printResults(List<org.tikv.kvproto.Kvrpcpb.KvPair> results)
      throws InvalidProtocolBufferException {
    for (Kvrpcpb.KvPair pair : results) {
      System.out.println(String.format("result : %s----\n%s",
          BlockLocationKey.parseFrom(pair.getKey().toByteArray()).toString(),
          BlockLocationValue.parseFrom(pair.getValue().toByteArray()).toString()));
    }
  }

  public static void main(String [] args) throws Exception {
    TiKVStoreBlockMetaRaw tiKVStoreBlockMetaRaw = new TiKVStoreBlockMetaRaw();

    BlockLocationKey mStartKey = BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION)
        .setFileId(0)
        .build();
    List<org.tikv.kvproto.Kvrpcpb.KvPair> results
        = tiKVStoreBlockMetaRaw.scan(mStartKey.toByteArray(),   1000);

    printResults(results);

    BlockLocationKey startKey = BlockLocationKey.newBuilder()
        .setTableType(KVStoreTable.BLOCK_LOCATION)
        .setFileId(67192750080L)
        .build();
    List<BlockLocationValue>  locations = tiKVStoreBlockMetaRaw.getLocations(startKey);

    tiKVStoreBlockMetaRaw.close();
  }
}