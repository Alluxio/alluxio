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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
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
    Assert.assertNull(options.getInputStream());
    Assert.assertFalse(options.isPrettyPrint());
    Assert.assertNull(options.getMD5());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    Object body = new Object();
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    InputStream inputStream = new ByteArrayInputStream(bytes);
    boolean prettyPrint = random.nextBoolean();
    TestCaseOptions options = TestCaseOptions.defaults();
    String md5 = RandomStringUtils.random(64);

    options.setBody(body);
    options.setInputStream(inputStream);
    options.setPrettyPrint(prettyPrint);
    options.setMD5(md5);

    Assert.assertEquals(body, options.getBody());
    Assert.assertEquals(inputStream, options.getInputStream());
    Assert.assertEquals(prettyPrint, options.isPrettyPrint());
    Assert.assertEquals(md5, options.getMD5());
  }

  @Test
  public void equals() throws Exception {
    CommonUtils.testEquals(TestCaseOptions.class, "mInputStream");
  }
}
