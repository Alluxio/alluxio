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

package tachyon.util.io;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;

public class PathUtilsTest {
  @Test
  public void concatPath() {
    Assert.assertEquals("/", PathUtils.concatPath("/"));
    Assert.assertEquals("/", PathUtils.concatPath("/", ""));
    Assert.assertEquals("/bar", PathUtils.concatPath("/", "bar"));

    Assert.assertEquals("foo", PathUtils.concatPath("foo"));
    Assert.assertEquals("/foo", PathUtils.concatPath("/foo"));
    Assert.assertEquals("/foo", PathUtils.concatPath("/foo", ""));

    // Join base without trailing "/"
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo", "bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo", "bar/"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo", "/bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo", "/bar/"));

    // Join base with trailing "/"
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar/"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "/bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "/bar/"));

    // Whitespace must be trimmed.
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo ", "bar  "));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo ", "  bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo ", "  bar  "));

    // Redundant separator must be trimmed.
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar//"));

    // Multiple components to join.
    Assert.assertEquals("/foo/bar/a/b/c", PathUtils.concatPath("/foo", "bar", "a", "b", "c"));
    Assert.assertEquals("/foo/bar/b/c", PathUtils.concatPath("/foo", "bar", "", "b", "c"));

    // Non-string
    Assert.assertEquals("/foo/bar/1", PathUtils.concatPath("/foo", "bar", 1));
    Assert.assertEquals("/foo/bar/2", PathUtils.concatPath("/foo", "bar", 2L));

    // Header
    Assert.assertEquals(Constants.HEADER + "host:port/foo/bar",
        PathUtils.concatPath(Constants.HEADER + "host:port", "/foo", "bar"));
  }
}
