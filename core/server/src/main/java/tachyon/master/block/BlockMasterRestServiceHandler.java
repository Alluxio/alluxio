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

package tachyon.master.block;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import tachyon.Constants;
import tachyon.exception.TachyonException;
import tachyon.master.TachyonMaster;

/**
 * This class is a REST handler for block master requests.
 */
@Path("/")
public final class BlockMasterRestServiceHandler {

  /**
   * Returns the service name.
   *
   * Method: GET
   * Response: {@code string}
   *
   * @return the response object
   */
  @GET
  @Path("block/service_name")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServiceName() {
    return Response.ok(Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME).build();

  }

  /**
   * Returns the service version.
   *
   * Method: GET
   * Response: {@code int}
   *
   * @return the response object
   */
  @GET
  @Path("block/service_version")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServiceVersion() {
    return Response.ok(Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param blockId the block id
   * @return the response object
   */
  @GET
  @Path("block/block_info")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBlockInfo(@QueryParam("blockId") long blockId) {
    BlockMaster master = TachyonMaster.get().getBlockMaster();
    try {
      return Response.ok(master.getBlockInfo(blockId)).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @return the response object
   */
  @GET
  @Path("block/capacity_bytes")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCapacityBytes() {
    BlockMaster master = TachyonMaster.get().getBlockMaster();
    return Response.ok(master.getCapacityBytes()).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path("block/used_bytes")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUsedBytes() {
    BlockMaster master = TachyonMaster.get().getBlockMaster();
    return Response.ok(master.getUsedBytes()).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path("block/worker_info_list")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWorkerInfoList() {
    BlockMaster master = TachyonMaster.get().getBlockMaster();
    return Response.ok(master.getWorkerInfoList()).build();
  }
}
