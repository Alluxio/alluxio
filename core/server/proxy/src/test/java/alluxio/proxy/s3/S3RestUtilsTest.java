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

package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class S3RestUtilsTest {

  private final List<String> mFakeFiles = ImmutableList.of(
      "/",
      "/a",
      "/a/b",
      "/a/b/c",
      "/a/b/c/d",
      "/a/b/cc",
      "/a/b/cc/a",
      "/a/b/cc/b",
      "/a/b/cc/c",
      "/a/b/cc/d",
      "/a/e",
      "/b"
  );

  private URIStatus createUriStatus(String path) {
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath(path);
    fileInfo.setFolder(true);
    return new URIStatus(fileInfo);
  }

  /**
   * Mock filesystem listStatus.
   */
  private List<String> listStatus(String path) {
    return mFakeFiles.stream()
        .filter(
            file -> file.startsWith(path) && file.length() > path.length() && !file.replaceFirst(
                path.equals("/") ? path : (path + "/"), "").contains("/"))
        .collect(Collectors.toList());
  }

  private List<String> listStatusByPrefix(String prefix, int maxKeys) {
    Function<AlluxioURI, List<URIStatus>> uriStatusProvider = uri -> listStatus(uri.getPath())
        .stream()
        .map(this::createUriStatus)
        .collect(Collectors.toList());
    return S3RestUtils.listStatusByPrefix(uriStatusProvider, prefix, maxKeys).stream()
        .map(URIStatus::getPath).sorted().collect(Collectors.toList());
  }

  @Test
  public void testListStatusByPrefix() {
    Assert.assertEquals(ImmutableList.of("/a"), listStatusByPrefix("/a", 1));
    Assert.assertEquals(ImmutableList.of("/a", "/a/b"), listStatusByPrefix("/a", 2));
    Assert.assertEquals(ImmutableList.of("/a", "/a/b", "/a/e"), listStatusByPrefix("/a", 3));
    Assert.assertEquals(ImmutableList.of("/a", "/a/b", "/a/b/c", "/a/e"),
        listStatusByPrefix("/a", 4));

    Assert.assertEquals(ImmutableList.of("/a/b"), listStatusByPrefix("/a/b", 1));
    Assert.assertEquals(ImmutableList.of("/a/b", "/a/b/c"), listStatusByPrefix("/a/b", 2));
    Assert.assertEquals(ImmutableList.of("/a/b", "/a/b/c", "/a/b/cc"),
        listStatusByPrefix("/a/b", 3));

    Assert.assertEquals(ImmutableList.of("/a/b/c"), listStatusByPrefix("/a/b/c", 1));
    Assert.assertEquals(ImmutableList.of("/a/b/c", "/a/b/cc"), listStatusByPrefix("/a/b/c", 2));
    Assert.assertEquals(
        ImmutableList.of("/a/b/c", "/a/b/c/d", "/a/b/cc", "/a/b/cc/a", "/a/b/cc/b", "/a/b/cc/c",
            "/a/b/cc/d"), listStatusByPrefix("/a/b/c", 10));
  }
}
