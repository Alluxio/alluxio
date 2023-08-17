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

import alluxio.test.util.CommonUtils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Tests for the {@link TestCaseOptions} class.
 */
public class TestCaseOptionsTest {
  /**
   * Tests that building a {@link TestCaseOptions} with the defaults works.
   */
  @Test
  public void defaults() {
    TestCaseOptions options = TestCaseOptions.defaults();

    Assert.assertNull(options.getBody());
    Assert.assertEquals(TestCaseOptions.OCTET_STREAM_CONTENT_TYPE, options.getContentType());
    Assert.assertEquals(StandardCharsets.UTF_8, options.getCharset());
    Assert.assertNull(options.getMD5());
    Assert.assertFalse(options.isPrettyPrint());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    String body = "body";
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    boolean prettyPrint = random.nextBoolean();
    TestCaseOptions options = TestCaseOptions.defaults();
    String md5 = RandomStringUtils.random(64);

    options.setBody(body);
    options.setCharset(StandardCharsets.US_ASCII);
    options.setPrettyPrint(prettyPrint);
    Assert.assertEquals(body, options.getBody());
    Assert.assertEquals(StandardCharsets.US_ASCII, options.getCharset());
    Assert.assertEquals(prettyPrint, options.isPrettyPrint());

    // Header fields
    options.setAuthorization("auth");
    options.setContentType(TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE);
    options.setMD5(md5);
    Assert.assertEquals("auth", options.getAuthorization());
    Assert.assertEquals("auth",
        options.getHeaders().get(TestCaseOptions.AUTHORIZATION_HEADER));
    Assert.assertEquals(TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE, options.getContentType());
    Assert.assertEquals(TestCaseOptions.TEXT_PLAIN_CONTENT_TYPE,
        options.getHeaders().get(TestCaseOptions.CONTENT_TYPE_HEADER));
    Assert.assertEquals(md5, options.getMD5());
    Assert.assertEquals(md5,
        options.getHeaders().get(TestCaseOptions.CONTENT_MD5_HEADER));
  }

  @Test
  public void equals() throws Exception {
    CommonUtils.testEquals(TestCaseOptions.class, "mCharset");

    Assert.assertEquals(TestCaseOptions.defaults().setCharset(StandardCharsets.US_ASCII),
        TestCaseOptions.defaults().setCharset(StandardCharsets.US_ASCII));
    Assert.assertNotEquals(TestCaseOptions.defaults().setCharset(StandardCharsets.US_ASCII),
        TestCaseOptions.defaults().setCharset(StandardCharsets.UTF_8));
  }
}
