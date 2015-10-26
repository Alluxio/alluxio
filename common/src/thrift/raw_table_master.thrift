namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

// Information about raw tables.
struct RawTableInfo {
  1: i64 id
  2: string name
  3: string path
  4: i32 columns
  5: binary metadata
}

service RawTableMasterService {

  // Tachyon Client API

  /**
   * Creates a raw table.
   */
  i64 createRawTable(1: string path, 2: i32 columns, 3: binary metadata)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Returns raw table information for the given id.
   */
  RawTableInfo getClientRawTableInfoById(1: i64 id)
    throws (1: exception.TachyonTException e)

  /**
   * Returns raw table information for the given path.
   */
  RawTableInfo getClientRawTableInfoByPath(1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Returns raw table id for the given path.
   */
  i64 getRawTableId(1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Updates raw table metadata.
   */
  void updateRawTableMetadata(1: i64 tableId, 2: binary metadata)
    throws (1: exception.TachyonTException e)
}
