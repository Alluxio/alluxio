/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.RestUtils;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.wire.LockBlockResult;
import alluxio.worker.AlluxioWorker;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.base.Preconditions;
import com.qmino.miredot.annotations.ReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for block worker requests.
 */
@NotThreadSafe
@Path(BlockWorkerClientRestServiceHandler.SERVICE_PREFIX)
public final class BlockWorkerClientRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SERVICE_PREFIX = "worker/block";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String ACCESS_BLOCK = "access_block";
  public static final String ASYNC_CHECKPOINT = "async_checkpoint";
  public static final String CACHE_BLOCK = "cache_block";
  public static final String CANCEL_BLOCK = "cancel_block";
  public static final String LOCK_BLOCK = "lock_block";
  public static final String PROMOTE_BLOCK = "promote_block";
  public static final String READ_BLOCK = "read_block";
  public static final String REQUEST_BLOCK_LOCATION = "request_block_location";
  public static final String REQUEST_SPACE = "request_space";
  public static final String UNLOCK_BLOCK = "unlock_block";
  public static final String WRITE_BLOCK = "write_block";

  private final BlockWorker mBlockWorker = AlluxioWorker.get().getBlockWorker();
  private final StorageTierAssoc mStorageTierAssoc =
      new WorkerStorageTierAssoc(WorkerContext.getConf());

  /**
   * @summary get the service name
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.String")
  public Response getServiceName() {
    // Need to encode the string as JSON because Jackson will not do it automatically.
    return RestUtils.createResponse(Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME);
  }

  /**
   * @summary get the service version
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.Long")
  public Response getServiceVersion() {
    return RestUtils.createResponse(Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION);
  }

  /**
   * @summary access a block
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(ACCESS_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.Void")
  public Response accessBlock(@QueryParam("blockId") Long blockId) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      mBlockWorker.accessBlock(Sessions.ACCESS_BLOCK_SESSION_ID, blockId);
      return RestUtils.createResponse();
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary asynchronously persist a file
   * @param fileId the file id
   * @return the response object
   */
  @POST
  @Path(ASYNC_CHECKPOINT)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.Boolean")
  public Response asyncCheckpoint(@QueryParam("fileId") Long fileId) {
    try {
      Preconditions.checkNotNull(fileId, "required 'fileId' parameter is missing");
      return RestUtils.createResponse(false);
    } catch (NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary cache a block
   * @param sessionId the session id
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(CACHE_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.Void")
  public Response cacheBlock(@QueryParam("sessionId") Long sessionId,
      @QueryParam("blockId") Long blockId) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      Preconditions.checkNotNull(sessionId, "required 'sessionId' parameter is missing");
      mBlockWorker.commitBlock(sessionId, blockId);
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary cancel a block
   * @param sessionId the session id
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(CANCEL_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.Void")
  public Response cancelBlock(@QueryParam("sessionId") Long sessionId,
      @QueryParam("blockId") Long blockId) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      Preconditions.checkNotNull(sessionId, "required 'sessionId' parameter is missing");
      mBlockWorker.abortBlock(sessionId, blockId);
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary lock a block
   * @param sessionId the session id
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(LOCK_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("alluxio.wire.LockBlockResult")
  public Response lockBlock(@QueryParam("sessionId") Long sessionId,
      @QueryParam("blockId") Long blockId) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      Preconditions.checkNotNull(sessionId, "required 'sessionId' parameter is missing");
      long lockId = mBlockWorker.lockBlock(sessionId, blockId);
      return RestUtils.createResponse(new LockBlockResult().setLockId(lockId)
          .setBlockPath(mBlockWorker.readBlock(sessionId, blockId, lockId)));
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary promote a block
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(PROMOTE_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.Void")
  public Response promoteBlock(@QueryParam("blockId") Long blockId) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      mBlockWorker.moveBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId,
          mStorageTierAssoc.getAlias(0));
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary read a block
   * @param sessionId the session id
   * @param blockId the block id
   * @param lockId the lock id
   * @param offset the offset to start the read at
   * @param length the number of bytes to read (the value -1 means read until EOF)
   * @return the response object
   */
  @GET
  @Path(READ_BLOCK)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @ReturnType("java.util.List<java.lang.Byte>")
  public Response readBlock(@QueryParam("sessionId") Long sessionId,
      @QueryParam("blockId") Long blockId, @QueryParam("lockId") Long lockId,
      @QueryParam("offset") Long offset, @QueryParam("length") Long length) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      Preconditions.checkNotNull(sessionId, "required 'sessionId' parameter is missing");
      Preconditions.checkNotNull(lockId, "required 'lockId' parameter is missing");
      Preconditions.checkNotNull(offset, "required 'offset' parameter is missing");
      Preconditions.checkNotNull(length, "required 'length' parameter is missing");
      Preconditions.checkState(offset >= 0, "invalid offset: %s", offset);
      Preconditions.checkState(length >= -1, "invalid length (except for -1): %s", length);
      // TODO(jiri): Wrap this logic in a block worker function; requires refactoring.
      BlockReader reader = null;
      try {
        reader = mBlockWorker.readBlockRemote(sessionId, blockId, lockId);
        long fileLength = reader.getLength();
        Preconditions
            .checkArgument(offset <= fileLength, "offset %s is larger than file length %s", offset,
                fileLength);
        Preconditions.checkArgument(length == -1 || offset + length <= fileLength,
            "offset %s plus length %s is larger than file length %s", offset, length, fileLength);
        long readLength = (length == -1) ? fileLength - offset : length;
        ByteBuffer buffer = reader.read(offset, readLength);
        mBlockWorker.accessBlock(sessionId, blockId);
        if (buffer.hasArray()) {
          return RestUtils.createResponse(buffer.array());
        }
        // We need to copy the bytes because the buffer byte array cannot be accessed directly.
        byte[] bytes = new byte[(int) readLength];
        buffer.get(bytes);
        return RestUtils.createResponse(bytes);
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    } catch (AlluxioException | IOException | IllegalStateException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary request a block location
   * @param sessionId the session id
   * @param blockId the block id
   * @param initialBytes the initial number of bytes to allocate
   * @return the response object
   */
  @POST
  @Path(REQUEST_BLOCK_LOCATION)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.String")
  public Response requestBlockLocation(@QueryParam("sessionId") Long sessionId,
      @QueryParam("blockId") Long blockId, @QueryParam("initialBytes") Long initialBytes) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      Preconditions.checkNotNull(sessionId, "required 'sessionId' parameter is missing");
      Preconditions.checkNotNull(initialBytes, "required 'initialBytes' parameter is missing");
      return RestUtils.createResponse(mBlockWorker
          .createBlock(sessionId, blockId, mStorageTierAssoc.getAlias(0), initialBytes));
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary request additional space for a block
   * @param sessionId the session id
   * @param blockId the block id
   * @param requestBytes the additional number of bytes to allocate
   * @return the response object
   */
  @POST
  @Path(REQUEST_SPACE)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.Void")
  public Response requestSpace(@QueryParam("sessionId") Long sessionId,
      @QueryParam("blockId") Long blockId, @QueryParam("requestBytes") Long requestBytes) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      Preconditions.checkNotNull(sessionId, "required 'sessionId' parameter is missing");
      Preconditions.checkNotNull(requestBytes, "required 'requestBytes' parameter is missing");
      mBlockWorker.requestSpace(sessionId, blockId, requestBytes);
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary unlock a block
   * @param sessionId the session id
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(UNLOCK_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  @ReturnType("java.lang.Void")
  public Response unlockBlock(@QueryParam("sessionId") Long sessionId,
      @QueryParam("blockId") Long blockId) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      Preconditions.checkNotNull(sessionId, "required 'sessionId' parameter is missing");
      mBlockWorker.unlockBlock(sessionId, blockId);
      return RestUtils.createResponse();
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary write a block
   * @param sessionId the session id
   * @param blockId the block id
   * @param offset the offset to start the read at
   * @param length the number of bytes to read (the value -1 means read until EOF)
   * @param data the data to write
   * @return the response object
   */
  @POST
  @Path(WRITE_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @ReturnType("java.lang.Void")
  public Response writeBlock(@QueryParam("sessionId") Long sessionId,
      @QueryParam("blockId") Long blockId, @QueryParam("offset") Long offset,
      @QueryParam("length") Long length, byte[] data) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      Preconditions.checkNotNull(sessionId, "required 'sessionId' parameter is missing");
      Preconditions.checkNotNull(offset, "required 'offset' parameter is missing");
      Preconditions.checkNotNull(length, "required 'length' parameter is missing");
      Preconditions.checkState(offset >= 0, "invalid offset: %s", offset);
      Preconditions.checkState(length >= -1, "invalid length (except for -1): %s", length);
      // TODO(jiri): Wrap this logic in a block worker function; requires refactoring.
      BlockWriter writer = null;
      try {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (offset == 0) {
          // This is the first write to the block, so create the temp block file. The file will only
          // be created if the first write starts at offset 0. This allocates enough space for the
          // write.
          mBlockWorker.createBlockRemote(sessionId, blockId, mStorageTierAssoc.getAlias(0), length);
        } else {
          // Allocate enough space in the existing temporary block for the write.
          mBlockWorker.requestSpace(sessionId, blockId, length);
        }
        writer = mBlockWorker.getTempBlockWriterRemote(sessionId, blockId);
        writer.append(buffer);
        return RestUtils.createResponse();
      } finally {
        if (writer != null) {
          writer.close();
        }
      }
    } catch (AlluxioException | IOException | IllegalStateException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }
}
