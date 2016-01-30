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

package tachyon.master.lineage;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.exception.TachyonException;
import tachyon.job.CommandLineJob;
import tachyon.job.JobConf;
import tachyon.master.TachyonMaster;

/**
 * This class is a REST handler for lineage master requests.
 */
@Path("/")
public final class LineageMasterRestServiceHandler {

  /**
   * @return the response object
   */
  @GET
  @Path("lineage/service_name")
  @Produces(MediaType.APPLICATION_JSON)
  public Response name() {
    return Response.ok(Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path("lineage/service_version")
  @Produces(MediaType.APPLICATION_JSON)
  public Response version() {
    return Response.ok(Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param inputFiles the lineage input files
   * @param outputFiles the lineage output files
   * @param command the job command
   * @param outputFile the job output file
   * @return the response object
   */
  @POST
  @Path("lineage/create_lineage")
  @Produces(MediaType.APPLICATION_JSON)
  public Response createLineage(@QueryParam("inputFiles") String inputFiles,
      @QueryParam("outputFiles") String outputFiles, @QueryParam("job.command") String command,
      @QueryParam("job.command.conf.outputFile") String outputFile) {
    LineageMaster master = TachyonMaster.get().getLineageMaster();
    List<TachyonURI> inputFilesUri = Lists.newArrayList();
    for (String path : inputFiles.split(":", -1)) {
      inputFilesUri.add(new TachyonURI(path));
    }
    List<TachyonURI> outputFilesUri = Lists.newArrayList();
    for (String path : outputFiles.split(":", -1)) {
      outputFilesUri.add(new TachyonURI(path));
    }
    CommandLineJob job = new CommandLineJob(command, new JobConf(outputFile));
    try {
      return Response.ok(master.createLineage(inputFilesUri, outputFilesUri, job)).build();
    } catch (TachyonException e) {
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
  @Path("lineage/delete_lineage")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteLineage(@QueryParam("lineageId") long lineageId,
      @QueryParam("cascade") boolean cascade) {
    LineageMaster master = TachyonMaster.get().getLineageMaster();
    try {
      return Response.ok(master.deleteLineage(lineageId, cascade)).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @return the response object
   */
  @GET
  @Path("lineage/lineage_info_list")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLineageInfoList() {
    LineageMaster master = TachyonMaster.get().getLineageMaster();
    try {
      return Response.ok(master.getLineageInfoList()).build();
    } catch (TachyonException e) {
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
  @Path("lineage/reinitialize_file")
  @Produces(MediaType.APPLICATION_JSON)
  public Response reinitializeFile(@QueryParam("path") String path,
      @QueryParam("blockSizeBytes") long blockSizeBytes, @QueryParam("ttl") long ttl) {
    LineageMaster master = TachyonMaster.get().getLineageMaster();
    try {
      return Response.ok(master.reinitializeFile(path, blockSizeBytes, ttl)).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path("lineage/report_lost_file")
  @Produces(MediaType.APPLICATION_JSON)
  public Response reportLostFile(@QueryParam("path") String path) {
    LineageMaster master = TachyonMaster.get().getLineageMaster();
    try {
      master.reportLostFile(path);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}
