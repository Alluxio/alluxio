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

package alluxio.proxy;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.web.ProxyWebServer;
import alluxio.wire.AlluxioProxyInfo;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.Map;
import java.util.TreeMap;

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
 * This class is a REST handler for requesting general proxy information.
 */
@NotThreadSafe
@Api(value = "/proxy", description = "Alluxio Proxy Rest Service")
@Path(AlluxioProxyRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioProxyRestServiceHandler {
  public static final String SERVICE_PREFIX = "proxy";

  // endpoints
  public static final String GET_INFO = "info";

  // queries
  public static final String QUERY_RAW_CONFIGURATION = "raw_configuration";

  private final ProxyProcess mProxyProcess;

  /**
   * Constructs a new {@link AlluxioProxyRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public AlluxioProxyRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mProxyProcess = (ProxyProcess) context
        .getAttribute(ProxyWebServer.ALLUXIO_PROXY_SERVLET_RESOURCE_KEY);
  }

  /**
   * @summary get the Alluxio proxy information
   * @param rawConfiguration if it's true, raw configuration values are returned,
   *    otherwise, they are looked up; if it's not provided in URL queries, then
   *    it is null, which means false.
   * @return the response object
   */
  @GET
  @Path(GET_INFO)
  @ApiOperation(value = "Get general Alluxio Proxy service information",
      response = alluxio.wire.AlluxioProxyInfo.class)
  public Response getInfo(@QueryParam(QUERY_RAW_CONFIGURATION) final Boolean rawConfiguration) {
    // TODO(jiri): Add a mechanism for retrieving only a subset of the fields.
    return RestUtils.call(() -> {
      boolean rawConfig = false;
      if (rawConfiguration != null) {
        rawConfig = rawConfiguration;
      }
      AlluxioProxyInfo result =
          new AlluxioProxyInfo()
              .setConfiguration(getConfigurationInternal(rawConfig))
              .setStartTimeMs(mProxyProcess.getStartTimeMs())
              .setUptimeMs(mProxyProcess.getUptimeMs())
              .setVersion(RuntimeConstants.VERSION);
      return result;
    }, ServerConfiguration.global());
  }

  private Map<String, String> getConfigurationInternal(boolean raw) {
    return new TreeMap<>(ServerConfiguration
        .toMap(ConfigurationValueOptions.defaults().useDisplayValue(true).useRawValue(raw)));
  }
}
