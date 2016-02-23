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

package alluxio.master.block;

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.master.AlluxioMaster;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for block master requests.
 */
@NotThreadSafe
@Path(BlockMasterClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
// TODO(jiri): Investigate auto-generation of REST API documentation.
public final class BlockMasterClientRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SERVICE_PREFIX = "block";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String GET_BLOCK_INFO = "block_info";
  public static final String GET_CAPACITY_BYTES = "capacity_bytes";
  public static final String GET_USED_BYTES = "used_bytes";
  public static final String GET_WORKER_INFO_LIST = "worker_info_list";

  private final BlockMaster mBlockMaster = AlluxioMaster.get().getBlockMaster();

  /**
   * @return the service name
   */
  @GET
  @Path(SERVICE_NAME)
  public Response getServiceName() {
    return Response.ok(Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the service version
   */
  @GET
  @Path(SERVICE_VERSION)
  public Response getServiceVersion() {
    return Response.ok(Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param blockId the block id
   * @return the block descriptor for the given id
   */
  @GET
  @Path(GET_BLOCK_INFO)
  public Response getBlockInfo(@QueryParam("blockId") Long blockId) {
    try {
      Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
      return Response.ok(mBlockMaster.getBlockInfo(blockId)).build();
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @return the total capacity (in bytes)
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  public Response getCapacityBytes() {
    return Response.ok(mBlockMaster.getCapacityBytes()).build();
  }

  /**
   * @return the used capacity (in bytes)
   */
  @GET
  @Path(GET_USED_BYTES)
  public Response getUsedBytes() {
    return Response.ok(mBlockMaster.getUsedBytes()).build();
  }

  /**
   * @return a list of worker descriptors for all workers
   */
  @GET
  @Path(GET_WORKER_INFO_LIST)
  public Response getWorkerInfoList() {
    return Response.ok(mBlockMaster.getWorkerInfoList()).build();
  }
}
