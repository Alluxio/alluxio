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

package alluxio.client.file.options;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.grpc.DeletePOptions;
import alluxio.test.util.CommonUtils;
import alluxio.util.grpc.GrpcUtils;

import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link DeleteOptions} class.
 */
public final class DeleteOptionsTest {
  /**
   * Tests that building a {@link DeleteOptions} with the defaults works.
   */
  @Test
  public void defaults() {
    DeleteOptions options = DeleteOptions.defaults();

    assertFalse(options.isRecursive());
    assertFalse(options.isAlluxioOnly());
    assertEquals(
        Configuration.getBoolean(PropertyKey.USER_FILE_DELETE_UNCHECKED),
        options.isUnchecked());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    boolean recursive = random.nextBoolean();
    boolean alluxioOnly = random.nextBoolean();
    boolean unchecked = random.nextBoolean();
    DeleteOptions options = DeleteOptions.defaults();

    options.setRecursive(recursive);
    options.setAlluxioOnly(alluxioOnly);
    options.setUnchecked(unchecked);
    assertEquals(recursive, options.isRecursive());
    assertEquals(alluxioOnly, options.isAlluxioOnly());
    assertEquals(unchecked, options.isUnchecked());
  }

  /**
   * Tests conversion to proto representation.
   */
  @Test
  public void toProto() {
    DeleteOptions options = DeleteOptions.defaults();
    DeletePOptions protoOptions = GrpcUtils.toProto(options);
    assertFalse(protoOptions.getRecursive());
    assertFalse(protoOptions.getAlluxioOnly());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(DeleteOptions.class);
  }
}
