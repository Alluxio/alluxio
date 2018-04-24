namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct GetTOptions {}
struct GetTResponse {
  1: binary data
}

struct GetNextKeysTOptions {}
struct GetNextKeysTResponse {
  1: list<binary> keys
}

struct GetSizeTOptions {}
struct GetSizeTResponse {
  1: i32 size
}

service KeyValueWorkerClientService extends common.AlluxioService {
  /**
   * Looks up a key in the block with the given block id.
   */
  GetTResponse get(
    /** the id of the block being accessed */ 1: i64 blockId,
    /** binary of the key */ 2: binary key,
    /** the method options */ 3: GetTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Gets a batch of keys next to the given key in the partition.
   * If current key is null, it means get the initial batch of keys.
   * If there are no more next keys, an empty list is returned.
   */
  GetNextKeysTResponse getNextKeys(
    /** the id of the partition */ 1: i64 blockId,
    /** current key */ 2: binary key,
    /** maximum number of keys to get */ 3: i32 numKeys,
    /** the method options */ 4: GetNextKeysTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Gets the number of key-value pairs in the partition.
   */
  GetSizeTResponse getSize(
    /** the id of the partition */ 1: i64 blockId,
    /** the method options */ 2: GetSizeTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
