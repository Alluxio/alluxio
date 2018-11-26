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
import alluxio.job.JobConfig;
import alluxio.job.ServiceConstants;
import alluxio.job.wire.JobInfo;
import alluxio.master.JobMasterProcess;
import alluxio.web.JobMasterWebServer;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.annotation.JacksonFeatures;

import java.util.List;

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
public final class JobMasterClientRestServiceHandler {
  private JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterClientRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public JobMasterClientRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mJobMaster = ((JobMasterProcess) context
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
    });
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
    });
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
    });
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
  public Response getStatus(@QueryParam("jobId") final long jobId) {
    return RestUtils.call(new RestUtils.RestCallable<JobInfo>() {
      @Override
      public JobInfo call() throws Exception {
        return mJobMaster.getStatus(jobId);
      }
    });
  }

  /**
   * Lists all the jobs in the history.
   *
   * @return the response of the names of all the jobs
   */
  @GET
  @Path(ServiceConstants.LIST)
  public Response list() {
    return RestUtils.call(new RestUtils.RestCallable<List<Long>>() {
      @Override
      public List<Long> call() throws Exception {
        return mJobMaster.list();
      }
    });
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
    });
  }
}
