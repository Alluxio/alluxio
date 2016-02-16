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

package alluxio.rest;

import alluxio.LocalAlluxioClusterResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import javax.ws.rs.core.Response;

/**
 * Represents a REST API test case.
 */
public class TestCase {
  private String mSuffix;
  private Map<String, String> mParameters;
  private String mMethod;
  private Object mExpectedResult;
  private String mService;
  private LocalAlluxioClusterResource mResource;

  /**
   * Creates a new instance of {@link TestCaseFactory}.
   *
   * @param suffix the suffix to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use
   * @param service the service to use
   * @param resource the local Alluxio cluster resource
   */
  protected TestCase(String suffix, Map<String, String> parameters, String method,
      Object expectedResult, String service, LocalAlluxioClusterResource resource) {
    mSuffix = suffix;
    mParameters = parameters;
    mMethod = method;
    mExpectedResult = expectedResult;
    mService = service;
    mResource = resource;
  }

  /**
   * @return the suffix
   */
  public String getSuffix() {
    return mSuffix;
  }

  /**
   * @return the method
   */
  public String getMethod() {
    return mMethod;
  }

  public URL createURL() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> parameter : mParameters.entrySet()) {
      sb.append(parameter.getKey() + "=" + parameter.getValue() + "&");
    }
    String hostname = "";
    int port = 0;
    if (mService == TestCaseFactory.MASTER_SERVICE) {
      hostname = mResource.get().getMasterHostname();
      port = mResource.get().getMaster().getWebLocalPort();
    }
    if (mService == TestCaseFactory.WORKER_SERVICE) {
      hostname = mResource.get().getWorkerAddress().getHost();
      port = mResource.get().getWorkerAddress().getWebPort();
    }
    return new URL("http://" + hostname + ":" + port + "/v1/api/" + mSuffix + "?" + sb.toString());
  }

  public String getResponse(HttpURLConnection connection) throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    StringBuffer response = new StringBuffer();

    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();

    return response.toString();
  }

  /**
   * Runs the test case.
   *
   * @throws Exception if an error occurs
   */
  public void run() throws Exception {
    HttpURLConnection connection = (HttpURLConnection) createURL().openConnection();
    connection.setRequestMethod(mMethod);
    connection.connect();
    Assert.assertEquals(mSuffix, Response.Status.OK.getStatusCode(), connection.getResponseCode());
    ObjectMapper mapper = new ObjectMapper();
    String expected = mapper.writeValueAsString(mExpectedResult);
    expected = expected.replaceAll("^\"|\"$", ""); // needed to handle string return values
    Assert.assertEquals(mSuffix, expected, getResponse(connection));
  }
}
