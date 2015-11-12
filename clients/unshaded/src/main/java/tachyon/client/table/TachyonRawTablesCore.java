package tachyon.client.table;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.thrift.RawTableInfo;

import java.nio.ByteBuffer;

@PublicApi
interface TachyonRawTablesCore {
  RawTable create(TachyonURI path, int numColumns, ByteBuffer metadata);

  RawTableInfo getInfo(RawTable rTable);

  RawTable open(TachyonURI path);

  void updateRawTableMetadata(RawTable table, ByteBuffer metadata);
}
