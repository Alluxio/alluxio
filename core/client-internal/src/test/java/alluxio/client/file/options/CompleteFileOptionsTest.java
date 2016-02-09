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

package alluxio.client.file.options;

import alluxio.thrift.CompleteFileTOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link CompleteFileOptions} class.
 */
public class CompleteFileOptionsTest {
  /**
   * Tests that building a {@link CompleteFileOptions} with the defaults works.
   */
  @Test
  public void defaultsTest() {
    CompleteFileOptions options = CompleteFileOptions.defaults();

    Assert.assertEquals(0, options.getUfsLength());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    long len = new Random().nextLong();
    CompleteFileOptions options = CompleteFileOptions.defaults();
    options.setUfsLength(len);
    Assert.assertEquals(len, options.getUfsLength());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThriftTest() {
    long len = new Random().nextLong();
    CompleteFileOptions options = CompleteFileOptions.defaults();
    options.setUfsLength(len);
    CompleteFileTOptions thriftOptions = options.toThrift();
    Assert.assertEquals(len, thriftOptions.getUfsLength());
  }
}
