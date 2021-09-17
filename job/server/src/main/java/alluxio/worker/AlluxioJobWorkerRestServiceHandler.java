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

package alluxio.worker;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.util.LogUtils;
import alluxio.web.JobWorkerWebServer;
import alluxio.wire.AlluxioJobWorkerInfo;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

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
 * This class is a REST handler for requesting general job worker information.
 */
@NotThreadSafe
@Api(value = "/job_worker", description = "Job Worker Rest Service")
@Path(AlluxioJobWorkerRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class  AlluxioJobWorkerRestServiceHandler {
  public static final String SERVICE_PREFIX = "job_worker";
  // log
  public static final String LOG_LEVEL = "logLevel";
  public static final String LOG_ARGUMENT_NAME = "logName";
  public static final String LOG_ARGUMENT_LEVEL = "level";

  // endpoints
  public static final String GET_INFO = "info";

  // queries
  public static final String QUERY_RAW_CONFIGURATION = "raw_configuration";

  private final JobWorkerProcess mJobWorker;

  /**
   * @param context context for the servlet
   */
  public AlluxioJobWorkerRestServiceHandler(@Context ServletContext context) {
    mJobWorker = (JobWorkerProcess) context
        .getAttribute(JobWorkerWebServer.ALLUXIO_JOB_WORKER_SERVLET_RESOURCE_KEY);
  }

  /**
   * @summary get the Alluxio job worker information
   * @param rawConfiguration if it's true, raw configuration values are returned,
   *    otherwise, they are looked up; if it's not provided in URL queries, then
   *    it is null, which means false.
   * @return the response object
   */
  @GET
  @Path(GET_INFO)
  @ApiOperation(value = "Get general job worker service information",
      response = alluxio.wire.AlluxioJobWorkerInfo.class)
  public Response getInfo(
      @ApiParam("Returns raw configuration values if true, false be default")
      @QueryParam(QUERY_RAW_CONFIGURATION) final Boolean rawConfiguration) {
    // TODO(jiri): Add a mechanism for retrieving only a subset of the fields.
    return RestUtils.call(() -> {
      boolean rawConfig = false;
      if (rawConfiguration != null) {
        rawConfig = rawConfiguration;
      }
      AlluxioJobWorkerInfo result =
          new AlluxioJobWorkerInfo()
              .setConfiguration(getConfigurationInternal(rawConfig))
              .setStartTimeMs(mJobWorker.getStartTimeMs())
              .setUptimeMs(mJobWorker.getUptimeMs())
              .setVersion(RuntimeConstants.VERSION);
      return result;
    }, ServerConfiguration.global());
  }

  /**
   * @summary set the Alluxio log information
   * @param logName the log's name
   * @param level the log level
   * @return the response object
   */
  @POST
  @Path(LOG_LEVEL)
  public Response logLevel(@QueryParam(LOG_ARGUMENT_NAME) final String logName,
                           @QueryParam(LOG_ARGUMENT_LEVEL) final String level) {
    return RestUtils.call(() -> LogUtils.setLogLevel(logName, level),
            ServerConfiguration.global());
  }

  private Map<String, String> getConfigurationInternal(boolean raw) {
    Set<Map.Entry<String, String>> properties = ServerConfiguration.toMap().entrySet();
    SortedMap<String, String> configuration = new TreeMap<>();
    for (Map.Entry<String, String> entry : properties) {
      String key = entry.getKey();
      if (PropertyKey.isValid(key)) {
        if (raw) {
          configuration.put(key, entry.getValue());
        } else {
          configuration.put(key, ServerConfiguration.get(PropertyKey.fromString(key)));
        }
      }
    }
    return configuration;
  }
}
