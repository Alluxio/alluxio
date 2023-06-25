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

package alluxio.worker.http;

import org.junit.Assert;
import org.junit.Test;

public class TestHttpServerHandler {

  @Test
  public void testGetRequestMapping() {
    String requestUri = "http://localhost:28080/page?"
        + "fileId=762445dac40c26f1d53d9e97e8984f7ab1ac4059618fa44aa73a485c63a1ff45&pageIndex=0";
    String requestMapping = getRequestMapping(requestUri);
    Assert.assertEquals("page", requestMapping);

    requestUri = "http://localhost:28080/files?"
        + "path=/alluxio/images";
    requestMapping = getRequestMapping(requestUri);
    Assert.assertEquals("files", requestMapping);
  }

  private String getRequestMapping(String requestUri) {
    int endIndex = requestUri.indexOf("?");
    int startIndex = requestUri.lastIndexOf("/", endIndex);
    String requestMapping = requestUri.substring(startIndex + 1, endIndex);
    return requestMapping;
  }
}
