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

package alluxio.master.file.options;

import alluxio.Configuration;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Unit tests for {@link CompleteFileOptions}.
 */
public class CompleteFileOptionsTest {

  /**
   * Tests the {@link alluxio.master.file.options.CompleteFileOptions.Builder}.
   */
  @Test
  public void builderTest() {
    Random random = new Random();
    long ufsLength = random.nextLong();
    long operationTimeMs = random.nextLong();

    CompleteFileOptions options =
        new CompleteFileOptions.Builder(new Configuration())
            .setUfsLength(ufsLength)
            .setOperationTimeMs(operationTimeMs)
            .build();

    Assert.assertEquals(ufsLength, options.getUfsLength());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
  }

  /**
   * Tests the {@link CompleteFileOptions#defaults()} method.
   */
  @Test
  public void defaultsTest() {
    CompleteFileOptions options = CompleteFileOptions.defaults();

    Assert.assertEquals(0, options.getUfsLength());
  }
}
