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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class TestHttpRequestUtil {

  @Parameterized.Parameters(name = "{index}-{0}")
  public static Collection<Object[]> requestUris() {
    return Arrays.asList(new Object[][] {
        {"http://127.0.0.1:28080/v1/file/5f2829f08879b0e89d07174cffa8d8"
            + "91bdf08ba9e91218e30fe39503dd42e32c/page/0/", false, -1L, -1L},
        {"http://127.0.0.1:28080/v1/file/5f2829f08879b0e89d07174cffa8d8"
            + "91bdf08ba9e91218e30fe39503dd42e32c/page/0", false, -1L, -1L},
        {"http://127.0.0.1:28080/v1/file/5f2829f08879b0e89d07174cffa8d8"
            + "91bdf08ba9e91218e30fe39503dd42e32c/page/0?offset=5&length=7", true, 5L, 7L},
    });
  }

  @Parameterized.Parameter()
  public String mRequestUri;

  @Parameterized.Parameter(1)
  public boolean mHasParametersMap;

  @Parameterized.Parameter(2)
  public long mOffset;

  @Parameterized.Parameter(3)
  public long mLength;

  @Test
  public void testExtractFieldsForGetPageApi() {
    List<String> fields = HttpRequestUtil.extractFieldsFromHttpRequestUri(mRequestUri);
    HttpRequestUri httpRequestUri = HttpRequestUri.of(fields);

    Assert.assertEquals("127.0.0.1", httpRequestUri.getHost());
    Assert.assertEquals(28080, httpRequestUri.getPort());
    Assert.assertEquals("v1", httpRequestUri.getVersion());
    Assert.assertEquals("file", httpRequestUri.getMappingPath());

    Assert.assertEquals(3, httpRequestUri.getRemainingFields().size());
    Assert.assertEquals("5f2829f08879b0e89d07174cffa8d891bdf08ba9e91218e30fe39503dd42e32c",
        httpRequestUri.getRemainingFields().get(0));
    Assert.assertEquals("page", httpRequestUri.getRemainingFields().get(1));
    Assert.assertEquals("0", httpRequestUri.getRemainingFields().get(2));

    boolean hasParametersMap = false;
    if (httpRequestUri.getParameters() != null) {
      hasParametersMap = true;
    }
    Assert.assertEquals(mHasParametersMap, hasParametersMap);

    if (mHasParametersMap) {
      Assert.assertEquals(mOffset, Long.parseLong(httpRequestUri.getParameters().get("offset")));
      Assert.assertEquals(mLength, Long.parseLong(httpRequestUri.getParameters().get("length")));
    }
  }

  @Test
  public void testExtractFieldsForListFilesApi() {
    String requestUri = "http://localhost:28080/v1/files?path=/";
    List<String> fields = HttpRequestUtil.extractFieldsFromHttpRequestUri(requestUri);
    HttpRequestUri httpRequestUri = HttpRequestUri.of(fields);

    Assert.assertEquals("localhost", httpRequestUri.getHost());
    Assert.assertEquals(28080, httpRequestUri.getPort());
    Assert.assertEquals("v1", httpRequestUri.getVersion());
    Assert.assertEquals("files", httpRequestUri.getMappingPath());
    Assert.assertEquals(0, httpRequestUri.getRemainingFields().size());
    Assert.assertNotNull(httpRequestUri.getParameters());
    Assert.assertEquals("/", httpRequestUri.getParameters().get("path"));
  }
}
