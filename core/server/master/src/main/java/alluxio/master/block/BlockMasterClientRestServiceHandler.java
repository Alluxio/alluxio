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

package alluxio.master.block;

import alluxio.Constants;
import alluxio.RestUtils;
import alluxio.master.MasterProcess;
import alluxio.web.MasterWebServer;
import alluxio.wire.BlockInfo;

import com.google.common.base.Preconditions;
import com.qmino.miredot.annotations.ReturnType;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for block master requests.
 *
 * @deprecated since version 1.4 and will be removed in version 2.0
 */
@NotThreadSafe
@Path(BlockMasterClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Deprecated
public final class BlockMasterClientRestServiceHandler {
  public static final String SERVICE_PREFIX = "master/block";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String GET_BLOCK_INFO = "block_info";

  private final BlockMaster mBlockMaster;

  /**
   * Constructs a new {@link BlockMasterClientRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public BlockMasterClientRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mBlockMaster = ((MasterProcess) context
        .getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY))
        .getMaster(BlockMaster.class);
  }

  /**
   * @summary get the service name
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  @ReturnType("java.lang.String")
  public Response getServiceName() {
    return RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME;
      }
    });
  }

  /**
   * @summary get the service version
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  @ReturnType("java.lang.Long")
  public Response getServiceVersion() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION;
      }
    });
  }

  /**
   * @summary get the block descriptor for a block
   * @param blockId the block id
   * @return the response object
   */
  @GET
  @Path(GET_BLOCK_INFO)
  @ReturnType("alluxio.wire.BlockInfo")
  public Response getBlockInfo(@QueryParam("blockId") final Long blockId) {
    return RestUtils.call(new RestUtils.RestCallable<BlockInfo>() {
      @Override
      public BlockInfo call() throws Exception {
        Preconditions.checkNotNull(blockId, "required 'blockId' parameter is missing");
        return mBlockMaster.getBlockInfo(blockId);
      }
    });
  }
}
