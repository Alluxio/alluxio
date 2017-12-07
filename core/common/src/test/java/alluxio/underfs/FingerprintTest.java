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

/**
 * Tests for the {@link Fingerprint} class.
 */
public final class FingerprintTest {

  @Test
  public void parseFileFingerprint() {
    FileFingerprint fp = new FileFingerprint(CommonUtils.randomAlphaNumString(10),
        CommonUtils.randomAlphaNumString(10), CommonUtils.randomAlphaNumString(10),
        CommonUtils.randomAlphaNumString(10), CommonUtils.randomAlphaNumString(10));
    String expected = fp.serialize();
    Assert.assertNotNull(expected);
    Assert.assertEquals(expected, Fingerprint.parse(expected).serialize());
  }

  @Test
  public void parseDirectoryFingerprint() {
    DirectoryFingerprint fp = new DirectoryFingerprint(CommonUtils.randomAlphaNumString(10),
        CommonUtils.randomAlphaNumString(10), CommonUtils.randomAlphaNumString(10),
        CommonUtils.randomAlphaNumString(10));
    String expected = fp.serialize();
    Assert.assertNotNull(expected);
    Assert.assertEquals(expected, Fingerprint.parse(expected).serialize());
  }

  @Test
  public void parseInvalidFingerprint() {
    InvalidFingerprint fp = new InvalidFingerprint();
    String expected = fp.serialize();
    Assert.assertNotNull(expected);
    Assert.assertEquals(expected, Fingerprint.parse(expected).serialize());
  }
}
