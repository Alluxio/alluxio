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

package alluxio.underfs;

import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link Fingerprint} class.
 */
public final class FingerprintTest {

  private Random mRandom = new Random();

  @Test
  public void parseFileFingerprint() {
    UfsStatus status = new UfsFileStatus(CommonUtils.randomAlphaNumString(10),
        CommonUtils.randomAlphaNumString(10), mRandom.nextLong(), mRandom.nextLong(),
        CommonUtils.randomAlphaNumString(10), CommonUtils.randomAlphaNumString(10),
        (short) mRandom.nextInt());
    Fingerprint fp = Fingerprint.create(CommonUtils.randomAlphaNumString(10), status);
    String expected = fp.serialize();
    Assert.assertNotNull(expected);
    Assert.assertEquals(expected, Fingerprint.parse(expected).serialize());
  }

  @Test
  public void parseDirectoryFingerprint() {
    UfsStatus status = new UfsDirectoryStatus(CommonUtils.randomAlphaNumString(10),
        CommonUtils.randomAlphaNumString(10), CommonUtils.randomAlphaNumString(10),
        (short) mRandom.nextInt());
    Fingerprint fp = Fingerprint.create(CommonUtils.randomAlphaNumString(10), status);
    String expected = fp.serialize();
    Assert.assertNotNull(expected);
    Assert.assertEquals(expected, Fingerprint.parse(expected).serialize());
  }

  @Test
  public void parseInvalidFingerprint() {
    Fingerprint fp = Fingerprint.create(CommonUtils.randomAlphaNumString(10), null);
    String expected = fp.serialize();
    Assert.assertNotNull(expected);
    Assert.assertEquals(expected, Fingerprint.parse(expected).serialize());
  }
}
