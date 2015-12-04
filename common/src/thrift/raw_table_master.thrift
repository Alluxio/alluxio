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

/**
 * This interface contains raw table master service endpoints for Tachyon clients.
 */
service RawTableMasterClientService extends common.TachyonService {

  /**
   * Creates a raw table and returns the raw table id.
   */
  i64 createRawTable( /** the RPC options */ 1: common.RpcOptions rpcOptions,
      /** the path of the raw table */ 2: string path,
      /** the number of columns */ 3: i32 columns,
      /** the metadata for the table */ 4: binary metadata)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Returns raw table information for the given id.
   */
  RawTableInfo getClientRawTableInfoById( /** the id of the table */ 1: i64 id)
    throws (1: exception.TachyonTException e)

  /**
   * Returns raw table information for the given path.
   */
  RawTableInfo getClientRawTableInfoByPath( /** the path of the table */ 1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Returns raw table id for the given path.
   */
  i64 getRawTableId( /** the path of the table */ 1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Updates raw table metadata.
   */
  void updateRawTableMetadata( /** the id of the table */ 1: i64 tableId,
      /** the metadata for the table */ 2: binary metadata)
    throws (1: exception.TachyonTException e)
}
