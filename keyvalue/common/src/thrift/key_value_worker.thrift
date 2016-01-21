namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

service KeyValueWorkerClientService extends common.TachyonService {
  /**
   * Looks up a key in the block with the given block id.
   */
  binary get( /** the id of the block being accessed */ 1: i64 blockId,
      /** binary of the key */ 2: binary key)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Gets a batch of keys next to the given key in the partition.
   * If current key is null, it means get the initial batch of keys.
   * If there are no more next keys, an empty list is returned.
   */
  list<binary> getNextKeys(/** the id of the partition */ 1: i64 blockId,
      /** current key */ 2: binary key, /** maximum number of keys to get */ 3: i32 numKeys)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Gets the number of key-value pairs in the partition.
   */
  i32 getSize(/** the id of the partition */ 1: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)
}
