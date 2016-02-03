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
// TODO(jiri): Figure out why Jersey complains if this is changed to "/block".
public final class BlockMasterRestServiceHandler {
  public static final String SERVICE_NAME = "block/service_name";
  public static final String SERVICE_VERSION = "block/service_version";
  public static final String GET_BLOCK_INFO = "block/block_info";
  public static final String GET_CAPACITY_BYTES = "block/capacity_bytes";
  public static final String GET_USED_BYTES = "block/used_bytes";
  public static final String GET_WORKER_INFO_LIST = "block/worker_info_list";

  private BlockMaster mBlockMaster = TachyonMaster.get().getBlockMaster();

  /**
   * Returns the service name.
   *
   * Method: GET
   * Response: {@code string}
   *
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
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
  @Path(SERVICE_VERSION)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServiceVersion() {
    return Response.ok(Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param blockId the block id
   * @return the response object
   */
  @GET
  @Path(GET_BLOCK_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBlockInfo(@QueryParam("blockId") long blockId) {
    try {
      return Response.ok(mBlockMaster.getBlockInfo(blockId)).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @return the response object
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCapacityBytes() {
    return Response.ok(mBlockMaster.getCapacityBytes()).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUsedBytes() {
    return Response.ok(mBlockMaster.getUsedBytes()).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path(GET_WORKER_INFO_LIST)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWorkerInfoList() {
    return Response.ok(mBlockMaster.getWorkerInfoList()).build();
  }
}
