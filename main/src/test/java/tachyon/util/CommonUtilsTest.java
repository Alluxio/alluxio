/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.util;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;

import java.net.UnknownHostException;
import java.util.UnknownFormatConversionException;

public class CommonUtilsTest {
  @Test
  public void parseMemorySizeTest() {
    long max = 10240;
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k / 10, CommonUtils.parseMemorySize(k / 10.0 + "b"));
      Assert.assertEquals(k / 10, CommonUtils.parseMemorySize(k / 10.0 + "B"));
      Assert.assertEquals(k / 10, CommonUtils.parseMemorySize(k / 10.0 + ""));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseMemorySize(k / 10.0 + "kb"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseMemorySize(k / 10.0 + "Kb"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseMemorySize(k / 10.0 + "KB"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseMemorySize(k / 10.0 + "kB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseMemorySize(k / 10.0 + "mb"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseMemorySize(k / 10.0 + "Mb"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseMemorySize(k / 10.0 + "MB"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseMemorySize(k / 10.0 + "mB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseMemorySize(k / 10.0 + "gb"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseMemorySize(k / 10.0 + "Gb"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseMemorySize(k / 10.0 + "GB"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseMemorySize(k / 10.0 + "gB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseMemorySize(k / 10.0 + "tb"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseMemorySize(k / 10.0 + "Tb"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseMemorySize(k / 10.0 + "TB"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseMemorySize(k / 10.0 + "tB"));
    }
    // We stop the pb test before 8192, since 8192 petabytes is beyond the scope of a java long.
    for (long k = 0; k < 8192; k ++) {
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseMemorySize(k / 10.0 + "pb"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseMemorySize(k / 10.0 + "Pb"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseMemorySize(k / 10.0 + "PB"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseMemorySize(k / 10.0 + "pB"));
    }
  }

  @Test
  public void replaceHostNameTest() throws UnknownHostException {
    Assert.assertEquals(CommonUtils.replaceHostName(""), null);
    Assert.assertEquals(CommonUtils.replaceHostName(null), null);

    String[] paths = new String[] {
        "hdfs://localhost:9000/dir",
        "hdfs://localhost/dir",
        "hdfs://localhost/",
        "hdfs://localhost",
        "file:///dir",
        "/dir",
        "anythingElse"
    };

    for (String path: paths) {
      Assert.assertEquals(CommonUtils.replaceHostName(path), path);
    }
  }

  @Test
  public void resolveHostNameTest() throws UnknownHostException {
    Assert.assertEquals(CommonUtils.resolveHostName(""), null);
    Assert.assertEquals(CommonUtils.resolveHostName(null), null);
    Assert.assertEquals(CommonUtils.resolveHostName("localhost"), "localhost");
  }
}
