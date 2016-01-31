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
public final class BlockWorkerRestServiceHandler {
  private StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc(WorkerContext.getConf());

  /**
   * @return the response object
   */
  @GET
  @Path("block/service_name")
  @Produces(MediaType.APPLICATION_JSON)
  public Response name() {
    return Response.ok(Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path("block/service_version")
  @Produces(MediaType.APPLICATION_JSON)
  public Response version() {
    return Response.ok(Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path("block/access_block")
  @Produces(MediaType.APPLICATION_JSON)
  public Response accessBlock(@QueryParam("blockId") long blockId) {
    BlockWorker worker = TachyonWorker.get().getBlockWorker();
    try {
      worker.accessBlock(Sessions.ACCESS_BLOCK_SESSION_ID, blockId);
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
  @Path("block/async_checkpoint")
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
  @Path("block/cache_block")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cacheBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    BlockWorker worker = TachyonWorker.get().getBlockWorker();
    try {
      worker.commitBlock(sessionId, blockId);
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
  @Path("block/cancel_block")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    BlockWorker worker = TachyonWorker.get().getBlockWorker();
    try {
      worker.abortBlock(sessionId, blockId);
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
  @Path("block/lock_block")
  @Produces(MediaType.APPLICATION_JSON)
  public Response lockBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    BlockWorker worker = TachyonWorker.get().getBlockWorker();
    try {
      long lockId = worker.lockBlock(sessionId, blockId);
      return Response.ok(
          new LockBlockResult().setLockId(lockId).setBlockPath(
              worker.readBlock(sessionId, blockId, lockId))).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param blockId the block id
   * @return the response object
   */
  @POST
  @Path("block/promote_block")
  @Produces(MediaType.APPLICATION_JSON)
  public Response promoteBlock(@QueryParam("blockId") long blockId) {
    BlockWorker worker = TachyonWorker.get().getBlockWorker();
    try {
      worker.moveBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId, mStorageTierAssoc.getAlias(0));
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
  @Path("block/request_block_location")
  @Produces(MediaType.APPLICATION_JSON)
  public Response requestBlockLocation(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId, @QueryParam("initialBytes") long initialBytes) {
    BlockWorker worker = TachyonWorker.get().getBlockWorker();
    try {
      return Response.ok(
          worker.createBlock(sessionId, blockId, mStorageTierAssoc.getAlias(0), initialBytes))
          .build();
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
  @Path("block/request_space")
  @Produces(MediaType.APPLICATION_JSON)
  public Response requestSpace(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId, @QueryParam("requestBytes") long requestBytes) {
    BlockWorker worker = TachyonWorker.get().getBlockWorker();
    try {
      worker.requestSpace(sessionId, blockId, requestBytes);
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
  @Path("block/unlock_block")
  @Produces(MediaType.APPLICATION_JSON)
  public Response unlockBlock(@QueryParam("sessionId") long sessionId,
      @QueryParam("blockId") long blockId) {
    BlockWorker worker = TachyonWorker.get().getBlockWorker();
    try {
      worker.unlockBlock(sessionId, blockId);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}
