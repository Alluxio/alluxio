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

package alluxio;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Rule;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

public abstract class AbstractRestApiTest {
  private static final String MASTER_SERVICE = "master";
  private static final String WORKER_SERVICE = "worker";

  protected class TestCase {
    public String mSuffix;
    public Map<String,String> mParameters;
    public String mMethod;
    public Object mExpectedResult;
    public String mService;

    protected TestCase(String suffix, Map<String, String> parameters, String method,
        Object expectedResult, String service) {
      mSuffix = suffix;
      mParameters = parameters;
      mMethod = method;
      mExpectedResult = expectedResult;
      mService = service;
    }

    public String getSuffix() {
      return mSuffix;
    }

    public Map<String, String> getParameters() {
      return mParameters;
    }

    public String getMethod() {
      return mMethod;
    }

    public Object getExpectedResult() {
      return mExpectedResult;
    }
  }

  public class MasterTestCase extends TestCase {
    public MasterTestCase(String suffix, Map<String, String> parameters, String method,
        Object expectedResult) {
      super(suffix, parameters, method, expectedResult, MASTER_SERVICE);
    }
  }

  public class WorkerTestCase extends TestCase {
    public WorkerTestCase(String suffix, Map<String, String> parameters, String method,
        Object expectedResult) {
      super(suffix, parameters, method, expectedResult, WORKER_SERVICE);
    }
  }

  @Rule
  protected LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  protected URL createURL(TestCase testCase) throws Exception {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> parameter : testCase.getParameters().entrySet()) {
      sb.append(parameter.getKey() + "=" + parameter.getValue() + "&");
    }
    if (testCase.mService == MASTER_SERVICE) {
      return new URL(
          "http://" + mResource.get().getMasterHostname() + ":" + mResource.get().getWorker()
              .getWebLocalPort() + "/v1/api/" + testCase.getSuffix() + "?" + sb.toString());
    }
    if (testCase.mService == WORKER_SERVICE) {
      return new URL(
          "http://" + mResource.get().getWorkerAddress().getHost() + ":" + mResource.get()
              .getWorker().getWebLocalPort() + "/v1/api/" + testCase.getSuffix() + "?" + sb
              .toString());
    }
    return null;
  }

  protected String getResponse(HttpURLConnection connection) throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    StringBuffer response = new StringBuffer();

    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();

    return response.toString();
  }

  public abstract void endpointsTest() throws Exception;

  protected void run(List<TestCase> testCases) throws Exception {
    for (TestCase testCase : testCases) {
      HttpURLConnection connection = (HttpURLConnection) createURL(testCase).openConnection();
      connection.setRequestMethod(testCase.getMethod());
      connection.connect();
      Assert.assertEquals(testCase.getSuffix(), connection.getResponseCode(),
          Response.Status.OK.getStatusCode());
      ObjectMapper mapper = new ObjectMapper();
      String expected = mapper.writeValueAsString(testCase.getExpectedResult());
      expected = expected.replaceAll("^\"|\"$", ""); // needed to handle string return values
      Assert.assertEquals(testCase.getSuffix(), expected, getResponse(connection));
    }
  }
}
