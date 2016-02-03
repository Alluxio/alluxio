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

package tachyon.worker.block;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.StorageTierAssoc;
import tachyon.WorkerStorageTierAssoc;
import tachyon.exception.TachyonException;
import tachyon.wire.LockBlockResult;
import tachyon.worker.TachyonWorker;
import tachyon.worker.WorkerContext;

/**
 * This class is a REST handler for block worker requests.
 */
@Path("/")
// TODO(jiri): Figure out why Jersey complains if this is changed to "/block".
public final class BlockWorkerRestServiceHandler {
  public static final String SERVICE_NAME = "block/service_name";
  public static final String SERVICE_VERSION = "block/service_version";
  public static final String ACCESS_BLOCK = "block/access_block";
  public static final String ASYNC_CHECKPOINT = "block/async_checkpoint";
  public static final String CACHE_BLOCK = "block/cache_block";
  public static final String CANCEL_BLOCK = "block/cancel_block";
  public static final String LOCK_BLOCK = "block/lock_block";
  public static final String PROMOTE_BLOCK = "block/promote_block";
  public static final String REQUEST_BLOCK_LOCATION = "block/request_block_location";
  public static final String REQUEST_SPACE = "block/request_space";
  public static final String UNLOCK_BLOCK = "block/unlock_block";

  private BlockWorker mBlockWorker = TachyonWorker.get().getBlockWorker();
  private StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc(WorkerContext.getConf());

  /**
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  @Produces(MediaType.APPLICATION_JSON)
  public Response name() {
    return Response.ok(Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  @Produces(MediaType.APPLICATION_JSON)
  public Response version() {
    return Response.ok(Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(ACCESS_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response accessBlock(@QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.accessBlock(Sessions.ACCESS_BLOCK_SESSION_ID, blockId);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param fileId the file id
   * @return the response object
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
   * @return the response object
   */
  @POST
  @Path(CACHE_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response cacheBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.commitBlock(sessionId, blockId);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(CANCEL_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.abortBlock(sessionId, blockId);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @return the response object
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
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(PROMOTE_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response promoteBlock(@QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.moveBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId,
          mStorageTierAssoc.getAlias(0));
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @param initialBytes the initial number of bytes to allocate
   * @return the response object
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
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @param requestBytes the additional number of bytes to allocate
   * @return the response object
   */
  @POST
  @Path(REQUEST_SPACE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response requestSpace(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId, @QueryParam("requestBytes") long requestBytes) {
    try {
      mBlockWorker.requestSpace(sessionId, blockId, requestBytes);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param sessionId the session id
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path(UNLOCK_BLOCK)
  @Produces(MediaType.APPLICATION_JSON)
  public Response unlockBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    try {
      mBlockWorker.unlockBlock(sessionId, blockId);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}
