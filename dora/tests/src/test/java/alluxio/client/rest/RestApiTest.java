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

package alluxio.client.rest;

import alluxio.Constants;
import alluxio.proxy.s3.ListBucketResult;
import alluxio.testutils.BaseIntegrationTest;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import java.security.MessageDigest;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.HttpMethod;

public abstract class RestApiTest extends BaseIntegrationTest {
  protected static final Map<String, String> NO_PARAMS = ImmutableMap.of();
  protected static final String TEST_USER_NAME = "testuser";
  protected static final String TEST_BUCKET_NAME = "bucket";
  protected String mHostname;
  protected int mPort;
  protected String mBaseUri = Constants.REST_API_PREFIX;

  protected TestCase newTestCase(String bucket, Map<String, String> params,
                                 String httpMethod, TestCaseOptions options) throws Exception {
    return new TestCase(mHostname, mPort, mBaseUri, bucket, params, httpMethod,
        options);
  }

  protected TestCase createBucketTestCase(String bucket) throws Exception {
    return newTestCase(bucket, NO_PARAMS, HttpMethod.PUT, getDefaultOptionsWithAuth());
  }

  protected TestCase createObjectTestCase(String bucket, byte[] object) throws Exception {
    return newTestCase(bucket, NO_PARAMS, HttpMethod.PUT, getDefaultOptionsWithAuth()
        .setBody(object)
        .setMD5(computeObjectChecksum(object)));
  }

  protected TestCase deleteTestCase(String uri) throws Exception {
    return newTestCase(uri, NO_PARAMS, HttpMethod.DELETE, getDefaultOptionsWithAuth());
  }

  protected TestCase headTestCase(String uri) throws Exception {
    return newTestCase(uri, NO_PARAMS, HttpMethod.HEAD, getDefaultOptionsWithAuth());
  }

  protected TestCase listTestCase(String uri, Map<String, String> params) throws Exception {
    return newTestCase(uri, params, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE));
  }

  protected void listStatusRestCall(Map<String, String> parameters, ListBucketResult expected)
      throws Exception {
    new TestCase(mHostname, mPort, mBaseUri,
        TEST_BUCKET_NAME, parameters, HttpMethod.GET,
        getDefaultOptionsWithAuth().setContentType(TestCaseOptions.XML_CONTENT_TYPE))
        .runAndCheckResult(expected);
  }

  protected TestCaseOptions getDefaultOptionsWithAuth(@NotNull String user) {
    return TestCaseOptions.defaults()
        .setAuthorization("AWS4-HMAC-SHA256 Credential=" + user + "/...");
  }

  protected TestCaseOptions getDefaultOptionsWithAuth() {
    return getDefaultOptionsWithAuth(TEST_USER_NAME);
  }

  private String computeObjectChecksum(byte[] objectContent) throws Exception {
    MessageDigest md5Hash = MessageDigest.getInstance("MD5");
    byte[] md5Digest = md5Hash.digest(objectContent);
    return BaseEncoding.base64().encode(md5Digest);
  }
}
