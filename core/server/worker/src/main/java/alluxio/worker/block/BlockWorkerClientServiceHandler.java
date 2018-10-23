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

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.thrift.AccessBlockTOptions;
import alluxio.thrift.AccessBlockTResponse;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.thrift.CacheBlockTOptions;
import alluxio.thrift.CacheBlockTResponse;
import alluxio.thrift.CancelBlockTOptions;
import alluxio.thrift.CancelBlockTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.LockBlockTOptions;
import alluxio.thrift.LockBlockTResponse;
import alluxio.thrift.PromoteBlockTOptions;
import alluxio.thrift.PromoteBlockTResponse;
import alluxio.thrift.RemoveBlockTOptions;
import alluxio.thrift.RemoveBlockTResponse;
import alluxio.thrift.RequestBlockLocationTOptions;
import alluxio.thrift.RequestBlockLocationTResponse;
import alluxio.thrift.RequestSpaceTOptions;
import alluxio.thrift.RequestSpaceTResponse;
import alluxio.thrift.SessionBlockHeartbeatTOptions;
import alluxio.thrift.SessionBlockHeartbeatTResponse;
import alluxio.thrift.UnlockBlockTOptions;
import alluxio.thrift.UnlockBlockTResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for block worker RPCs invoked by an Alluxio client. These RPCs are
 * no longer supported as of 1.5.0. All methods will throw {@link UnsupportedOperationException}.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class BlockWorkerClientServiceHandler implements BlockWorkerClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerClientServiceHandler.class);
  private static final String UNSUPPORTED_MESSAGE = "Unsupported as of version 1.5.0";

  /**
   * Creates a new instance of {@link BlockWorkerClientServiceHandler}.
   *
   * @param worker block worker handler
   */
  public BlockWorkerClientServiceHandler(BlockWorker worker) {}

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public AccessBlockTResponse accessBlock(final long blockId, AccessBlockTOptions options)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public CacheBlockTResponse cacheBlock(final long sessionId, final long blockId,
      CacheBlockTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public CancelBlockTResponse cancelBlock(final long sessionId, final long blockId,
      CancelBlockTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public LockBlockTResponse lockBlock(long blockId, long sessionId, LockBlockTOptions options)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public PromoteBlockTResponse promoteBlock(final long blockId, PromoteBlockTOptions options)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public RemoveBlockTResponse removeBlock(final long blockId, RemoveBlockTOptions options)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public RequestBlockLocationTResponse requestBlockLocation(final long sessionId,
      final long blockId, final long initialBytes, final int writeTier,
      RequestBlockLocationTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public RequestSpaceTResponse requestSpace(final long sessionId, final long blockId,
      final long requestBytes, RequestSpaceTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public SessionBlockHeartbeatTResponse sessionBlockHeartbeat(final long sessionId,
      final List<Long> metrics, SessionBlockHeartbeatTOptions options) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public UnlockBlockTResponse unlockBlock(long blockId, long sessionId, UnlockBlockTOptions options)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }
}
