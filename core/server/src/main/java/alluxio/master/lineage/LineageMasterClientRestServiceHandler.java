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

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import alluxio.master.AlluxioMaster;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for lineage master requests.
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
// TODO(jiri): Figure out why Jersey complains if this is changed to "/lineage".
public final class LineageMasterClientRestServiceHandler {
  public static final String SERVICE_NAME = "lineage/service_name";
  public static final String SERVICE_VERSION = "lineage/service_version";
  public static final String CREATE_LINEAGE = "lineage/create_lineage";
  public static final String DELETE_LINEAGE = "lineage/delete_lineage";
  public static final String GET_LINEAGE_INFO_LIST = "lineage/lineage_info_list";
  public static final String REINITIALIZE_FILE = "lineage/reinitialize_file";
  public static final String REPORT_LOST_FILE = "lineage/report_lost_file";

  private LineageMaster mLineageMaster = AlluxioMaster.get().getLineageMaster();

  /**
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  public Response name() {
    return Response.ok(Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  public Response version() {
    return Response.ok(Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param inputFiles a colon-separated list of the lineage input files
   * @param outputFiles a colon-separated list of the lineage output files
   * @param command the job command
   * @param outputFile the job output file
   * @return the response object
   */
  @POST
  @Path(CREATE_LINEAGE)
  public Response createLineage(@QueryParam("inputFiles") String inputFiles,
      @QueryParam("outputFiles") String outputFiles, @QueryParam("job.command") String command,
      @QueryParam("job.command.conf.outputFile") String outputFile) {
    List<AlluxioURI> inputFilesUri = Lists.newArrayList();
    for (String path : inputFiles.split(":", -1)) {
      inputFilesUri.add(new AlluxioURI(path));
    }
    List<AlluxioURI> outputFilesUri = Lists.newArrayList();
    for (String path : outputFiles.split(":", -1)) {
      outputFilesUri.add(new AlluxioURI(path));
    }
    CommandLineJob job = new CommandLineJob(command, new JobConf(outputFile));
    try {
      return Response.ok(mLineageMaster.createLineage(inputFilesUri, outputFilesUri, job)).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param lineageId the lineage id
   * @param cascade whether to delete lineage recursively
   * @return the response object
   */
  @POST
  @Path(DELETE_LINEAGE)
  public Response deleteLineage(@QueryParam("lineageId") long lineageId,
      @QueryParam("cascade") boolean cascade) {
    try {
      return Response.ok(mLineageMaster.deleteLineage(lineageId, cascade)).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @return the response object
   */
  @GET
  @Path(GET_LINEAGE_INFO_LIST)
  public Response getLineageInfoList() {
    try {
      return Response.ok(mLineageMaster.getLineageInfoList()).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @param blockSizeBytes the file block size (in bytes)
   * @param ttl the file time-to-live (in seconds)
   * @return the response object
   */
  @POST
  @Path(REINITIALIZE_FILE)
  public Response reinitializeFile(@QueryParam("path") String path,
      @QueryParam("blockSizeBytes") long blockSizeBytes, @QueryParam("ttl") long ttl) {
    try {
      return Response.ok(mLineageMaster.reinitializeFile(path, blockSizeBytes, ttl)).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(REPORT_LOST_FILE)
  public Response reportLostFile(@QueryParam("path") String path) {
    try {
      mLineageMaster.reportLostFile(path);
      return Response.ok().build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}
