package alluxio;

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
public class RestApiTestCase {
  private String mSuffix;
  private Map<String, String> mParameters;
  private String mMethod;
  private Object mExpectedResult;
  private String mService;
  private LocalAlluxioClusterResource mResource;

  /**
   * Creates a new instance of {@link RestApiTestCaseFactory}.
   *
   * @param suffix the suffix to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use
   * @param service the service to use
   * @param resource the local Alluxio cluster resource
   */
  protected RestApiTestCase(String suffix, Map<String, String> parameters, String method,
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
    if (mService == RestApiTestCaseFactory.MASTER_SERVICE) {
      hostname = mResource.get().getMasterHostname();
      port = mResource.get().getMaster().getWebLocalPort();
    }
    if (mService == RestApiTestCaseFactory.WORKER_SERVICE) {
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
