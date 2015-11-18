namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

/**
 * Information about raw tables.
 */
struct RawTableInfo {
  1: i64 id
  2: string name
  3: string path
  4: i32 columns
  5: binary metadata
}

service RawTableMasterService extends common.TachyonService {

  /**
   * Creates a raw table.
   * @param rpcOptions
   * @param path
   * @param columns
   * @param metadata
   * @return the raw table id
   * @throws TachyonTException
   * @throws ThriftIOException
   */
  i64 createRawTable(1: common.RpcOptions rpcOptions, 2: string path, 3: i32 columns, 
      4: binary metadata)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Returns raw table information for the given id.
   * @param id
   * @return raw table information for the given id
   * @throws TachyonTException
   */
  RawTableInfo getClientRawTableInfoById(1: i64 id)
    throws (1: exception.TachyonTException e)

  /**
   * Returns raw table information for the given path.
   * @param path
   * @return raw table information for the given path
   * @throws TachyonTException
   */
  RawTableInfo getClientRawTableInfoByPath(1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Returns raw table id for the given path.
   * @param path
   * @return raw table id for the given path
   * @throws TachyonTException
   */
  i64 getRawTableId(1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Updates raw table metadata.
   * @param tableId
   * @param metadata
   * @throws TachyonTException
   */
  void updateRawTableMetadata(1: i64 tableId, 2: binary metadata)
    throws (1: exception.TachyonTException e)
}
