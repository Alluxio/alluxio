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

package alluxio.underfs.b2;

import com.backblaze.b2.client.structures.B2Allowed;
import com.backblaze.b2.client.structures.B2Capabilities;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests for {@link B2Utils} methods.
 */
public final class B2UtilsTest {

  private static final String BUCKET_ID = "123456789012";
  private static final String BUCKET_NAME = "foo";

  @Test
  public void translateUserReadPermission() {
    // Setup listBuckets, listFiles, readFiles, shareFiles
    B2Allowed readAllowed = new B2Allowed(Arrays
        .asList(B2Capabilities.LIST_BUCKETS, B2Capabilities.LIST_FILES, B2Capabilities.READ_FILES,
            B2Capabilities.SHARE_FILES), BUCKET_ID, BUCKET_NAME, "");
    // Assert
    Assert.assertEquals((short) 0500, B2Utils.translateBucketAcl(readAllowed.getCapabilities()));
  }

  @Test
  public void translateUserWritePermission() {
    // Setup deleteFiles, listBuckets, writeFiles
    B2Allowed writeAllowed = new B2Allowed(Arrays
        .asList(B2Capabilities.DELETE_FILES, B2Capabilities.LIST_BUCKETS,
            B2Capabilities.WRITE_FILES), BUCKET_ID, BUCKET_NAME, "");
    // Assert
    Assert.assertEquals((short) 0200, B2Utils.translateBucketAcl(writeAllowed.getCapabilities()));
  }

  @Test
  public void translateUserFullPermission() {
    // Setup deleteFiles, listBuckets, listFiles, readFiles, shareFiles, writeFiles
    B2Allowed fullAllowed = new B2Allowed(Arrays
        .asList(B2Capabilities.DELETE_FILES, B2Capabilities.LIST_BUCKETS, B2Capabilities.LIST_FILES,
            B2Capabilities.READ_FILES, B2Capabilities.SHARE_FILES, B2Capabilities.WRITE_FILES),
        BUCKET_ID, BUCKET_NAME, "");
    // Assert
    Assert.assertEquals((short) 0700, B2Utils.translateBucketAcl(fullAllowed.getCapabilities()));
  }
}
