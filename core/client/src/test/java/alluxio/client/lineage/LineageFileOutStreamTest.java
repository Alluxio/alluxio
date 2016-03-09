/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.client.UnderStorageType;
import alluxio.client.file.options.OutStreamOptions;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * Tests {@link LineageFileOutStream}.
 */
public final class LineageFileOutStreamTest {

  /**
   * Tests that the correct {@link UnderStorageType} is set when creating the stream.
   *
   * @throws Exception if creating the stream fails
   */
  @Test
  public void outStreamCreationTest() throws Exception {
    LineageFileOutStream stream =
        new LineageFileOutStream(new AlluxioURI("/path"), OutStreamOptions.defaults());
    UnderStorageType underStorageType =
        (UnderStorageType) Whitebox.getInternalState(stream, "mUnderStorageType");
    Assert.assertEquals(UnderStorageType.ASYNC_PERSIST, underStorageType);
  }
}
