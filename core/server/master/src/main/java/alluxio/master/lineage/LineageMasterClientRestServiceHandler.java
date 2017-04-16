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

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RestUtils;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import alluxio.master.AlluxioMasterService;
import alluxio.web.MasterWebServer;
import alluxio.wire.LineageInfo;
import alluxio.wire.TtlAction;

import com.google.common.base.Preconditions;
import com.qmino.miredot.annotations.ReturnType;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for lineage master requests.
 *
 * @deprecated since version 1.4 and will be removed in version 2.0
 */
@NotThreadSafe
@Path(LineageMasterClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Deprecated
public final class LineageMasterClientRestServiceHandler {
  public static final String SERVICE_PREFIX = "master/lineage";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String CREATE_LINEAGE = "create_lineage";
  public static final String DELETE_LINEAGE = "delete_lineage";
  public static final String GET_LINEAGE_INFO_LIST = "lineage_info_list";
  public static final String REINITIALIZE_FILE = "reinitialize_file";
  public static final String REPORT_LOST_FILE = "report_lost_file";

  private final LineageMaster mLineageMaster;

  /**
   * Constructs a new {@link LineageMasterClientRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public LineageMasterClientRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mLineageMaster = ((AlluxioMasterService) context
        .getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY))
        .getMaster(LineageMaster.class);
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
        return Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME;
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
        return Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION;
      }
    });
  }

  /**
   * @summary create a lineage
   * @param inputFiles a colon-separated list of the lineage input files
   * @param outputFiles a colon-separated list of the lineage output files
   * @param command the job command
   * @param outputFile the job output file
   * @return the response object
   */
  @POST
  @Path(CREATE_LINEAGE)
  @ReturnType("java.lang.Long")
  public Response createLineage(@QueryParam("inputFiles") final String inputFiles,
      @QueryParam("outputFiles") final String outputFiles,
      @QueryParam("command") final String command,
      @QueryParam("commandOutputFile") final String outputFile) {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        Preconditions.checkNotNull(inputFiles, "required 'inputFiles' parameter is missing");
        Preconditions.checkNotNull(outputFiles, "required 'outputFiles' parameter is missing");
        Preconditions.checkNotNull(command, "required 'command' parameter is missing");
        Preconditions.checkNotNull(outputFile, "required 'commandOutputFile' parameter is missing");
        List<AlluxioURI> inputFilesUri = new ArrayList<>();
        for (String path : inputFiles.split(":", -1)) {
          inputFilesUri.add(new AlluxioURI(path));
        }
        List<AlluxioURI> outputFilesUri = new ArrayList<>();
        for (String path : outputFiles.split(":", -1)) {
          outputFilesUri.add(new AlluxioURI(path));
        }
        CommandLineJob job = new CommandLineJob(command, new JobConf(outputFile));
        return mLineageMaster.createLineage(inputFilesUri, outputFilesUri, job);
      }
    });
  }

  /**
   * @summary delete a lineage
   * @param lineageId the lineage id
   * @param cascade whether to delete lineage recursively
   * @return the response object
   */
  @POST
  @Path(DELETE_LINEAGE)
  @ReturnType("java.lang.Boolean")
  public Response deleteLineage(@QueryParam("lineageId") final Long lineageId,
      @QueryParam("cascade") final boolean cascade) {
    return RestUtils.call(new RestUtils.RestCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Preconditions.checkNotNull(lineageId, "required 'lineageId' parameter is missing");
        return mLineageMaster.deleteLineage(lineageId, cascade);
      }
    });
  }

  /**
   * @summary get the list of lineage descriptors
   * @return the response object
   */
  @GET
  @Path(GET_LINEAGE_INFO_LIST)
  @ReturnType("java.util.List<alluxio.wire.LineageInfo>")
  public Response getLineageInfoList() {
    return RestUtils.call(new RestUtils.RestCallable<List<LineageInfo>>() {
      @Override
      public List<LineageInfo> call() throws Exception {
        return mLineageMaster.getLineageInfoList();
      }
    });
  }

  /**
   * @summary reinitialize a file
   * @param path the file path
   * @param blockSizeBytes the file block size (in bytes)
   * @param ttl the file time-to-live (in seconds)
   * @param ttlAction action to take after TTL is expired
   * @return the response object
   */
  @POST
  @Path(REINITIALIZE_FILE)
  @ReturnType("java.lang.Long")
  public Response reinitializeFile(@QueryParam("path") final String path,
      @QueryParam("blockSizeBytes") final Long blockSizeBytes, @QueryParam("ttl") final Long ttl,
      @QueryParam("ttlAction") final TtlAction ttlAction) {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        Preconditions
            .checkNotNull(blockSizeBytes, "required 'blockSizeBytes' parameter is missing");
        Preconditions.checkNotNull(ttl, "required 'ttl' parameter is missing");
        return mLineageMaster.reinitializeFile(path, blockSizeBytes, ttl, ttlAction);
      }
    });
  }

  /**
   * @summary report a lost file
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(REPORT_LOST_FILE)
  @ReturnType("java.lang.Void")
  public Response reportLostFile(@QueryParam("path") final String path) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        mLineageMaster.reportLostFile(path);
        return null;
      }
    });
  }
}
