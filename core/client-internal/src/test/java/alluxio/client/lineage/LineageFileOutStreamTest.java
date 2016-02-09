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
