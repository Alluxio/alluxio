/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.worker.block;

import alluxio.Constants;
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
// TODO(jiri): Investigate auto-generation of REST API documentation.
public final class BlockWorkerClientRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SERVICE_PREFIX = "block";
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
   * @return the service name
   */
  @GET
  @Path(SERVICE_NAME)
  @Produces(MediaType.APPLICATION_JSON)
  public Response name() {
    return Response.ok(Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the service version
   */
  @GET
  @Path(SERVICE_VERSION)
  @Produces(MediaType.APPLICATION_JSON)
  public Response version() {
    return Response.ok(Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param blockId the block id
   * @return status 200 on success
   */
  @POST
  @Path(ACCESS_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response accessBlock(@QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.accessBlock(Sessions.ACCESS_BLOCK_SESSION_ID, blockId);
      return Response.ok().build();
    } catch (AlluxioException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param fileId the file id
   * @return whether the operation succeeded
   */
  @POST
  @Path(ASYNC_CHECKPOINT)
  @Produces(MediaType.APPLICATION_JSON)
  public Response asyncCheckpoint(@QueryParam("fileId") long fileId) {
    return Response.ok(false).build();
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @return status 200 on success
   */
  @POST
  @Path(CACHE_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response cacheBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.commitBlock(sessionId, blockId);
      return Response.ok().build();
    } catch (AlluxioException | IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @return status 200 on success
   */
  @POST
  @Path(CANCEL_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.abortBlock(sessionId, blockId);
      return Response.ok().build();
    } catch (AlluxioException | IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @return the lock block result
   */
  @POST
  @Path(LOCK_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response lockBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    try {
      long lockId = mBlockWorker.lockBlock(sessionId, blockId);
      return Response.ok(
          new LockBlockResult().setLockId(lockId).setBlockPath(
              mBlockWorker.readBlock(sessionId, blockId, lockId))).build();
    } catch (AlluxioException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param blockId the block id
   * @return status 200 on success
   */
  @POST
  @Path(PROMOTE_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response promoteBlock(@QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.moveBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId,
          mStorageTierAssoc.getAlias(0));
      return Response.ok().build();
    } catch (AlluxioException | IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @param lockId the lock id
   * @param offset the offset to start the read at
   * @param length the number of bytes to read (the value -1 means read until EOF)
   * @return the requested bytes
   */
  @GET
  @Path(READ_BLOCK)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response readBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId, @QueryParam("lockId") long lockId,
      @QueryParam("offset") long offset, @QueryParam("length") long length) {
    // TODO(jiri): Wrap this logic in a block worker function; requires refactoring.
    Preconditions.checkState(offset >= 0, "invalid offset: %s", offset);
    Preconditions.checkState(length >= -1, "invalid length (except for -1): %s", length);

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
      return Response.ok(buffer.array()).build();
    } catch (AlluxioException | IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOG.warn(e.getMessage());
        // TODO(jiri): Decide how to handle this.
      }
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @param initialBytes the initial number of bytes to allocate
   * @return a string representing the path to the local file
   */
  @POST
  @Path(REQUEST_BLOCK_LOCATION)
  @Produces(MediaType.APPLICATION_JSON)
  public Response requestBlockLocation(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId, @QueryParam("initialBytes") long initialBytes) {
    try {
      return Response
          .ok(mBlockWorker.createBlock(sessionId, blockId, mStorageTierAssoc.getAlias(0),
              initialBytes)).build();
    } catch (AlluxioException | IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @param requestBytes the additional number of bytes to allocate
   * @return status 200 on success
   */
  @POST
  @Path(REQUEST_SPACE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response requestSpace(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId, @QueryParam("requestBytes") long requestBytes) {
    try {
      mBlockWorker.requestSpace(sessionId, blockId, requestBytes);
      return Response.ok().build();
    } catch (AlluxioException | IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @return status 200 on success
   */
  @POST
  @Path(UNLOCK_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response unlockBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.unlockBlock(sessionId, blockId);
      return Response.ok().build();
    } catch (AlluxioException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @param offset the offset to start the read at
   * @param length the number of bytes to read (the value -1 means read until EOF)
   * @param data the data to write
   * @return status 200 on success
   */
  @POST
  @Path(WRITE_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response writeBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId, @QueryParam("offset") long offset,
      @QueryParam("length") long length, byte[] data) {
    // TODO(jiri): Wrap this logic in a block worker function; requires refactoring.
    Preconditions.checkState(offset >= 0, "invalid offset: %s", offset);
    Preconditions.checkState(length >= -1, "invalid length (except for -1): %s", length);

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
      return Response.ok().build();
    } catch (AlluxioException | IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException e) {
        LOG.warn(e.getMessage());
        // TODO(jiri): Decide how to handle this.
      }
    }
  }
}
