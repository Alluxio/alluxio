package tachyon.client.table;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.exception.TachyonException;
import tachyon.thrift.RawTableInfo;

import java.io.IOException;
import java.nio.ByteBuffer;

@PublicApi
interface TachyonRawTablesCore {
  SimpleRawTable create(TachyonURI path, int numColumns, ByteBuffer metadata) throws IOException,
      TachyonException;

  RawTableInfo getInfo(SimpleRawTable rawTable) throws IOException, TachyonException;

  SimpleRawTable open(TachyonURI path) throws IOException, TachyonException;

  void updateRawTableMetadata(SimpleRawTable rawTable, ByteBuffer metadata) throws IOException,
      TachyonException;
}
