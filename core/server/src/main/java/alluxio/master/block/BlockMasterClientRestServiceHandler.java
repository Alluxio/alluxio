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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.master.AlluxioMaster;
import alluxio.master.MasterContext;
import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Preconditions;
import com.qmino.miredot.annotations.ReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

  public static final String SERVICE_PREFIX = "master/block";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String GET_BLOCK_INFO = "block_info";
  public static final String GET_CAPACITY_BYTES = "capacity_bytes";
  public static final String GET_DISK_CAPACITY_BYTES = "disk_capacity_bytes";
  public static final String GET_DISK_FREE_BYTES = "disk_free_bytes";
  public static final String GET_DISK_USED_BYTES = "disk_used_bytes";
  public static final String GET_FREE_BYTES = "free_bytes";
  public static final String GET_TOTAL_BYTES_ON_TIERS = "total_bytes_on_tiers";
  public static final String GET_USED_BYTES = "used_bytes";
  public static final String GET_USED_BYTES_ON_TIERS = "used_bytes_on_tiers";
  public static final String GET_WORKER_COUNT = "worker_count";
  public static final String GET_WORKER_INFO_LIST = "worker_info_list";

  private final BlockMaster mBlockMaster = AlluxioMaster.get().getBlockMaster();
  private final Configuration mMasterConf = MasterContext.getConf();
  private final String mUfsRoot = mMasterConf.get(Constants.UNDERFS_ADDRESS);
  private final UnderFileSystem mUfs = UnderFileSystem.get(mUfsRoot, mMasterConf);

  /**
   * @summary get the service name
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  @ReturnType("java.lang.String")
  public Response getServiceName() {
    return Response.ok(Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @summary get the service version
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  @ReturnType("java.lang.Long")
  public Response getServiceVersion() {
    return Response.ok(Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @summary get the block descriptor for a block
   * @param blockId the block id
   * @return the response object
   */
  @GET
  @Path(GET_BLOCK_INFO)
  @ReturnType("alluxio.wire.BlockInfo")
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
   * @summary get the total capacity
   * @return the response object
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  public Response getCapacityBytes() {
    return Response.ok(mBlockMaster.getCapacityBytes()).build();
  }

  /**
   * @summary get the total disk capacity, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_DISK_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  public Response getDiskCapacityBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @summary get the free disk capacity, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_DISK_FREE_BYTES)
  @ReturnType("java.lang.Long")
  public Response getFreeDiskCapacityBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_FREE)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @summary get the used disk capacity, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_DISK_USED_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUsedDiskCapacityBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_USED)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @summary get the free capacity
   * @return the response object
   */
  @GET
  @Path(GET_FREE_BYTES)
  @ReturnType("java.lang.Long")
  public Response getFreeCapacityBytes() {
    return Response.ok(mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes()).build();
  }

  /**
   * @summary get the mapping from tier alias to total capacity of the tier
   * @return the response object
   */
  @GET
  @Path(GET_TOTAL_BYTES_ON_TIERS)
  @ReturnType("java.util.Map<String, Long>")
  public Response getTotalBytesOnTiers() {
    return Response.ok(mBlockMaster.getTotalBytesOnTiers()).build();
  }

  /**
   * @summary get the used capacity
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUsedBytes() {
    return Response.ok(mBlockMaster.getUsedBytes()).build();
  }

  /**
   * @summary get the mapping from tier alias to used capacity of the tier
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES_ON_TIERS)
  @ReturnType("java.util.Map<String, Long>")
  public Response getUsedBytesOnTiers() {
    return Response.ok(mBlockMaster.getUsedBytesOnTiers()).build();
  }

  /**
   * @summary get the count of workers
   * @return the response object
   */
  @GET
  @Path(GET_WORKER_COUNT)
  @ReturnType("java.lang.Integer")
  public Response getWorkerCount() {
    return Response.ok(mBlockMaster.getWorkerCount()).build();
  }

  /**
   * @summary get the list of worker descriptors
   * @return the response object
   */
  @GET
  @Path(GET_WORKER_INFO_LIST)
  @ReturnType("java.util.List<alluxio.wire.WorkerInfo>")
  public Response getWorkerInfoList() {
    return Response.ok(mBlockMaster.getWorkerInfoList()).build();
  }
}
