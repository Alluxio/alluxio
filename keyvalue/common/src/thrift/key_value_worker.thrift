namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

service KeyValueWorkerClientService extends common.TachyonService {
  /**
   * Accesses a block given the block id.
   */
  binary get( /** the id of the block being accessed */ 1: i64 blockId,
      /** binary of the key */ 2: binary key)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Gets a list of all keys available in the partition.
   */
  list<binary> getAllKeys(/** the id of the partition */ 1: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Gets the number of (key, value) pairs in the partition.
   */
  i32 getSize(/** the id of the partition */ 1: i64 blockId))
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)
}
