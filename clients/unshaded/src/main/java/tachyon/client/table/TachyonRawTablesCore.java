package tachyon.client.table;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.exception.TachyonException;
import tachyon.thrift.RawTableInfo;

import java.io.IOException;
import java.nio.ByteBuffer;

@PublicApi
interface TachyonRawTablesCore {
  RawTable create(TachyonURI path, int numColumns, ByteBuffer metadata) throws IOException,
      TachyonException;

  RawTableInfo getInfo(RawTable rawTable) throws IOException, TachyonException;

  RawTable open(TachyonURI path) throws IOException, TachyonException;

  void updateRawTableMetadata(RawTable rawTable, ByteBuffer metadata) throws IOException,
      TachyonException;
}
