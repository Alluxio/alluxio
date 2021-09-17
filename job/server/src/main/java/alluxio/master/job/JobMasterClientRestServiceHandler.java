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

package alluxio.master.job;

import alluxio.Constants;
import alluxio.RestUtils;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.ListAllPOptions;
import alluxio.job.JobConfig;
import alluxio.job.ServiceConstants;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.AlluxioJobMasterProcess;
import alluxio.web.JobMasterWebServer;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.annotation.JacksonFeatures;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * The REST service handler for job master.
 */
@Path(ServiceConstants.MASTER_SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/master")
public final class JobMasterClientRestServiceHandler {
  private JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterClientRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public JobMasterClientRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mJobMaster = ((AlluxioJobMasterProcess) context
        .getAttribute(JobMasterWebServer.ALLUXIO_JOB_MASTER_SERVLET_RESOURCE_KEY)).getJobMaster();
  }

  /**
   * @return the service name
   */
  @GET
  @Path(ServiceConstants.SERVICE_NAME)
  public Response getServiceName() {
    // Need to encode the string as JSON because Jackson will not do it automatically.
    return RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return Constants.JOB_MASTER_CLIENT_SERVICE_NAME;
      }
    }, ServerConfiguration.global());
  }

  /**
   * @return the service version
   */
  @GET
  @Path(ServiceConstants.SERVICE_VERSION)
  public Response getServiceVersion() {
    return RestUtils.call(new RestUtils.RestCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return Constants.JOB_MASTER_CLIENT_SERVICE_VERSION;
      }
    }, ServerConfiguration.global());
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job to cancel
   * @return the response
   */
  @POST
  @Path(ServiceConstants.CANCEL)
  public Response cancel(@QueryParam("jobId") final long jobId) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        mJobMaster.cancel(jobId);
        return null;
      }
    }, ServerConfiguration.global());
  }

  /**
   * Gets the job status.
   *
   * @param jobId the job id
   * @return the response of the job status
   */
  @GET
  @Path(ServiceConstants.GET_STATUS)
  @JacksonFeatures(serializationEnable = {SerializationFeature.INDENT_OUTPUT})
  @ApiOperation(value = "Gets the status of a job", response = JobInfo.class)
  public Response getStatus(@QueryParam("jobId") final long jobId) {
    return RestUtils.call(new RestUtils.RestCallable<JobInfo>() {
      @Override
      public JobInfo call() throws Exception {
        return mJobMaster.getStatus(jobId);
      }
    }, ServerConfiguration.global());
  }

  /**
   * Lists all the jobs in the history.
   * @param statusList the target status of jobs
   * @param name the name of jobs
   *
   * @return the response of the names of all the jobs
   */
  @GET
  @Path(ServiceConstants.LIST)
  public Response list(@QueryParam("status") final List<String> statusList,
      @QueryParam("name") final String name) {
    return RestUtils.call(new RestUtils.RestCallable<List<Long>>() {
      @Override
      public List<Long> call() throws Exception {
        if (statusList != null) {
          return mJobMaster.list(ListAllPOptions.newBuilder()
              .addAllStatus(statusList.stream()
                  .map(Status::valueOf)
                  .map(Status::toProto)
                  .collect(Collectors.toList()))
              .setName(Objects.toString(name, ""))
              .build());
        } else {
          return mJobMaster.list(ListAllPOptions.getDefaultInstance());
        }
      }
    }, ServerConfiguration.global());
  }

  /**
   * Runs a job.
   *
   * @param jobConfig the configuration of the job
   * @return the job id that tracks the job
   */
  @POST
  @Path(ServiceConstants.RUN)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response run(final JobConfig jobConfig) {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mJobMaster.run(jobConfig);
      }
    }, ServerConfiguration.global());
  }
}
