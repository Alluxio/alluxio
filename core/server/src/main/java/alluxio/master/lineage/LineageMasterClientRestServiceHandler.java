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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
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
@NotThreadSafe
@Path(LineageMasterClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
// TODO(jiri): Investigate auto-generation of REST API documentation.
public final class LineageMasterClientRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SERVICE_PREFIX = "lineage";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String CREATE_LINEAGE = "create_lineage";
  public static final String DELETE_LINEAGE = "delete_lineage";
  public static final String GET_LINEAGE_INFO_LIST = "lineage_info_list";
  public static final String REINITIALIZE_FILE = "reinitialize_file";
  public static final String REPORT_LOST_FILE = "report_lost_file";

  private final LineageMaster mLineageMaster = AlluxioMaster.get().getLineageMaster();

  /**
   * @return the service name
   */
  @GET
  @Path(SERVICE_NAME)
  public Response name() {
    return Response.ok(Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the service version
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
   * @return the id of the created lineage
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
    } catch (AlluxioException | IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param lineageId the lineage id
   * @param cascade whether to delete lineage recursively
   * @return true if the lineage is deleted, false otherwise
   */
  @POST
  @Path(DELETE_LINEAGE)
  public Response deleteLineage(@QueryParam("lineageId") long lineageId,
      @QueryParam("cascade") boolean cascade) {
    try {
      return Response.ok(mLineageMaster.deleteLineage(lineageId, cascade)).build();
    } catch (AlluxioException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @return a list of lineage descriptors for all lineages
   */
  @GET
  @Path(GET_LINEAGE_INFO_LIST)
  public Response getLineageInfoList() {
    try {
      return Response.ok(mLineageMaster.getLineageInfoList()).build();
    } catch (AlluxioException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param path the file path
   * @param blockSizeBytes the file block size (in bytes)
   * @param ttl the file time-to-live (in seconds)
   * @return the id of the reinitialized file when the file is lost or not completed, -1 otherwise
   */
  @POST
  @Path(REINITIALIZE_FILE)
  public Response reinitializeFile(@QueryParam("path") String path,
      @QueryParam("blockSizeBytes") long blockSizeBytes, @QueryParam("ttl") long ttl) {
    try {
      return Response.ok(mLineageMaster.reinitializeFile(path, blockSizeBytes, ttl)).build();
    } catch (AlluxioException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @param path the file path
   * @return status 200 on success
   */
  @POST
  @Path(REPORT_LOST_FILE)
  public Response reportLostFile(@QueryParam("path") String path) {
    try {
      mLineageMaster.reportLostFile(path);
      return Response.ok().build();
    } catch (AlluxioException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }
}
