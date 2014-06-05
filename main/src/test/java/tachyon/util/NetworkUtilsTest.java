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

import java.net.UnknownHostException;

public class NetworkUtilsTest {

  @Test
  public void replaceHostNameTest() throws UnknownHostException {
    Assert.assertEquals(NetworkUtils.replaceHostName(""), null);
    Assert.assertEquals(NetworkUtils.replaceHostName(null), null);

    String[] paths =
        new String[] { "hdfs://localhost:9000/dir", "hdfs://localhost/dir", "hdfs://localhost/",
            "hdfs://localhost", "file:///dir", "/dir", "anythingElse" };

    for (String path : paths) {
      Assert.assertEquals(NetworkUtils.replaceHostName(path), path);
    }
  }

  @Test
  public void resolveHostNameTest() throws UnknownHostException {
    Assert.assertEquals(NetworkUtils.resolveHostName(""), null);
    Assert.assertEquals(NetworkUtils.resolveHostName(null), null);
    Assert.assertEquals(NetworkUtils.resolveHostName("localhost"), "localhost");
  }
}
