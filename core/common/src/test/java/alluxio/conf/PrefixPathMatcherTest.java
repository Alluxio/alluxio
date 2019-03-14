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

package alluxio.conf;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests {@link PrefixPathMatcher}.
 */
public class PrefixPathMatcherTest {
  @Test
  public void match() {
    String[] paths = new String[]{
        "/",
        "/a/b",
        "/a/b/",
        "/a/b/c",
        "/a/bc",
    };
    Set<String> pathSet = new HashSet<>();
    for (String path : paths) {
      pathSet.add(path);
    }
    PrefixPathMatcher matcher = new PrefixPathMatcher(pathSet);
    Assert.assertEquals("", matcher.match(""));
    Assert.assertEquals("/", matcher.match("/"));
    Assert.assertEquals("/", matcher.match("/a"));
    Assert.assertEquals("/", matcher.match("/b"));
    Assert.assertEquals("/", matcher.match("/ab"));
    Assert.assertEquals("/a/b", matcher.match("/a/b"));
    Assert.assertEquals("/a/b/", matcher.match("/a/b/"));
    Assert.assertEquals("/a/b/", matcher.match("/a/b/d"));
    Assert.assertEquals("/a/bc", matcher.match("/a/bc"));
    Assert.assertEquals("/", matcher.match("/a/bd"));
    Assert.assertEquals("/a/b/c", matcher.match("/a/b/c"));
    Assert.assertEquals("/a/b/c", matcher.match("/a/b/c/d"));
    Assert.assertEquals("/a/b/", matcher.match("/a/b/cd"));
  }
}
