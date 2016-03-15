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

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Version;

import com.qmino.miredot.annotations.ReturnType;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for requesting general master information.
 */
@NotThreadSafe
@Path(AlluxioMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
// TODO(cc): Investigate auto-generation of REST API documentation.
public final class AlluxioMasterRestServiceHandler {
  public static final String SERVICE_PREFIX = "master";
  public static final String GET_DEBUG = "debug";
  public static final String GET_ADDRESS = "address";
  public static final String GET_START_TIME_MS = "start_time_ms";
  public static final String GET_UPTIME_MS = "uptime_ms";
  public static final String GET_VERSION = "version";

  private final AlluxioMaster mMaster = AlluxioMaster.get();
  private final Configuration mMasterConf = MasterContext.getConf();

  /**
   * @summary get whether the master is in debug mode
   * @return the response object
   */
  @GET
  @Path(GET_DEBUG)
  @ReturnType("java.lang.Boolean")
  public Response isDebug() {
    return Response.ok(mMasterConf.getBoolean(Constants.DEBUG)).build();
  }

  /**
   * @summary get the master address
   * @return the response object
   */
  @GET
  @Path(GET_ADDRESS)
  @ReturnType("java.lang.String")
  public Response getAddress() {
    return Response.ok(mMasterConf.get(Constants.MASTER_ADDRESS)).build();
  }

  /**
   * @summary get the uptime of the master
   * @return the response object
   */
  @GET
  @Path(GET_UPTIME_MS)
  @ReturnType("java.lang.Long")
  public Response getUptimeMs() {
    return Response.ok(System.currentTimeMillis() - mMaster.getStarttimeMs()).build();
  }

  /**
   * @summary get the start time of the master
   * @return the response object
   */
  @GET
  @Path(GET_START_TIME_MS)
  @ReturnType("java.lang.Long")
  public Response getStartTimeMs() {
    return Response.ok(mMaster.getStarttimeMs()).build();
  }

  /**
   * @summary get the version of the master
   * @return the response object
   */
  @GET
  @Path(GET_VERSION)
  @ReturnType("java.lang.String")
  public Response getVersion() {
    return Response.ok(Version.VERSION).build();
  }
}
