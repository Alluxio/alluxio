/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.keyvalue;

import alluxio.RpcUtils;
import alluxio.Sessions;
import alluxio.client.keyvalue.ByteBufferKeyValuePartitionReader;
import alluxio.client.keyvalue.Index;
import alluxio.client.keyvalue.PayloadReader;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.grpc.GetNextKeysPRequest;
import alluxio.grpc.GetNextKeysPResponse;
import alluxio.grpc.GetPRequest;
import alluxio.grpc.GetPResponse;
import alluxio.grpc.GetSizePRequest;
import alluxio.grpc.GetSizePResponse;
import alluxio.grpc.KeyValueWorkerClientServiceGrpc;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * RPC service handler on worker side to read a local key-value block.
 */
// TODO(binfan): move logic outside and make this a simple wrapper.
@ThreadSafe
public final class KeyValueWorkerClientServiceHandler
    extends KeyValueWorkerClientServiceGrpc.KeyValueWorkerClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyValueWorkerClientServiceHandler.class);

  /** BlockWorker handler for access block info. */
  private final BlockWorker mBlockWorker;

  /**
   * @param blockWorker the {@link BlockWorker}
   */
  KeyValueWorkerClientServiceHandler(BlockWorker blockWorker) {
    mBlockWorker = Preconditions.checkNotNull(blockWorker, "blockWorker");
  }

  @Override
  public void get(GetPRequest request, StreamObserver<GetPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetPResponse>) () -> {
      ByteBuffer value =
          getInternal(request.getBlockId(), request.getKey().asReadOnlyByteBuffer());
      GetPResponse.Builder response = GetPResponse.newBuilder();
      if (value == null) {
        response.setData(ByteString.copyFrom(new byte[0]));
      } else {
        response.setData(ByteString.copyFrom(value.array()));
      }
      return response.build();
    }, "get", "request=%s", responseObserver, request);
  }

  @Override
  public void getNextKeys(GetNextKeysPRequest request,
      StreamObserver<GetNextKeysPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetNextKeysPResponse>) () -> {
      final long sessionId = Sessions.KEYVALUE_SESSION_ID;
      final long lockId = mBlockWorker.lockBlock(sessionId, request.getBlockId());
      GetNextKeysPResponse.Builder response = GetNextKeysPResponse.newBuilder();
      try {
        ByteBufferKeyValuePartitionReader reader =
            getReader(sessionId, lockId, request.getBlockId());
        Index index = reader.getIndex();
        PayloadReader payloadReader = reader.getPayloadReader();

        List<ByteString> ret = Lists.newArrayListWithExpectedSize(request.getNumKeys());
        ByteBuffer currentKey = request.getKey().asReadOnlyByteBuffer();
        for (int i = 0; i < request.getNumKeys(); i++) {
          ByteBuffer nextKey = index.nextKey(currentKey, payloadReader);
          if (nextKey == null) {
            break;
          }
          ret.add(ByteString.copyFrom(copyAsNonDirectBuffer(nextKey)));
          currentKey = nextKey;
        }
        response.addAllKeys(ret);
      } catch (InvalidWorkerStateException e) {
        // We shall never reach here
        LOG.error("Reaching invalid state to get all keys", e);
      } finally {
        mBlockWorker.unlockBlock(lockId);
      }
      return response.build();
    }, "getNextKeys", "request=%s", responseObserver, request);
  }

  // TODO(cc): Try to remove the duplicated try-catch logic in other methods like getNextKeys.
  @Override
  public void getSize(GetSizePRequest request, StreamObserver<GetSizePResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetSizePResponse>) () -> {
      final long sessionId = Sessions.KEYVALUE_SESSION_ID;
      final long lockId = mBlockWorker.lockBlock(sessionId, request.getBlockId());
      GetSizePResponse.Builder response = GetSizePResponse.newBuilder().setSize(0);
      try {
        response.setSize(getReader(sessionId, lockId, request.getBlockId()).size());
      } catch (InvalidWorkerStateException e) {
        // We shall never reach here
        LOG.error("Reaching invalid state to get size", e);
      } finally {
        mBlockWorker.unlockBlock(lockId);
      }
      return response.build();
    }, "getSize", "request=%s", responseObserver, request);
  }

  private ByteBuffer copyAsNonDirectBuffer(ByteBuffer directBuffer) {
    // Thrift assumes the ByteBuffer returned has array() method, which is not true if the
    // ByteBuffer is direct. We make a non-direct copy of the ByteBuffer to return.
    return BufferUtils.cloneByteBuffer(directBuffer);
  }

  /**
   * Internal logic to get value from the given block.
   *
   * @param blockId Block Id
   * @param keyBuffer bytes of key
   * @return key found in the key-value block or null if not found
   * @throws BlockDoesNotExistException if the worker is not serving this block
   */
  private ByteBuffer getInternal(long blockId, ByteBuffer keyBuffer)
      throws BlockDoesNotExistException, IOException {
    final long sessionId = Sessions.KEYVALUE_SESSION_ID;
    final long lockId = mBlockWorker.lockBlock(sessionId, blockId);
    try {
      return getReader(sessionId, lockId, blockId).get(keyBuffer);
    } catch (InvalidWorkerStateException e) {
      // We shall never reach here
      LOG.error("Reaching invalid state to get a key", e);
    } finally {
      mBlockWorker.unlockBlock(lockId);
    }
    return null;
  }

  private ByteBufferKeyValuePartitionReader getReader(long sessionId, long lockId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    BlockReader blockReader = mBlockWorker.readBlockRemote(sessionId, blockId, lockId);
    ByteBuffer fileBuffer = blockReader.read(0, blockReader.getLength());
    ByteBufferKeyValuePartitionReader reader = new ByteBufferKeyValuePartitionReader(fileBuffer);
    // TODO(binfan): clean fileBuffer which is a direct byte buffer

    blockReader.close();
    return reader;
  }
}
