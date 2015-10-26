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

package tachyon.master.file.options;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import tachyon.conf.TachyonConf;

public class CompleteFileOptionsTest {
  @Test
  public void builderTest() {
    Random random = new Random();
    long length = random.nextLong();
    long operationTimeMs = random.nextLong();

    CompleteFileOptions options =
        new CompleteFileOptions.Builder(new TachyonConf())
            .setLength(length)
            .setOperationTimeMs(operationTimeMs)
            .build();

    Assert.assertEquals(length, options.getLength());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
  }

  @Test
  public void defaultsTest() {
    CompleteFileOptions options = CompleteFileOptions.defaults();

    Assert.assertEquals(0, options.getLength());
  }
}
