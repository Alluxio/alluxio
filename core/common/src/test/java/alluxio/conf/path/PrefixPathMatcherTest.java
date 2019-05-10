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

package alluxio.conf.path;

import alluxio.AlluxioURI;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Tests {@link PrefixPathMatcher}.
 */
public class PrefixPathMatcherTest {
  @Test
  public void empty() {
    PrefixPathMatcher matcher = new PrefixPathMatcher(Collections.emptySet());
    checkNoMatch(matcher, "/");
    checkNoMatch(matcher, "/a");
    checkNoMatch(matcher, "/a/bc");
  }

  @Test
  public void root() {
    Set<String> paths = new HashSet<>();
    paths.add("/");
    PrefixPathMatcher matcher = new PrefixPathMatcher(paths);
    checkMatch(matcher, "/", "/");
    checkMatch(matcher, "/a", "/");
    checkMatch(matcher, "/a/b", "/");
  }

  @Test
  public void match() {
    String[] paths = new String[]{
        "/a",
        "/a/b/",
        "/a/b/c",
        "/a/bc",
    };
    Set<String> pathSet = new HashSet<>();
    Collections.addAll(pathSet, paths);
    PrefixPathMatcher matcher = new PrefixPathMatcher(pathSet);
    checkNoMatch(matcher, "/");
    checkNoMatch(matcher, "/ab");
    checkNoMatch(matcher, "/b");
    checkMatch(matcher, "/a", "/a");
    checkMatch(matcher, "/a/", "/a");
    checkMatch(matcher, "/a/bd", "/a");
    checkMatch(matcher, "/a/b", "/a/b/", "/a");
    checkMatch(matcher, "/a/b/", "/a/b/", "/a");
    checkMatch(matcher, "/a/b/d", "/a/b/", "/a");
    checkMatch(matcher, "/a/b/cd", "/a/b/", "/a");
    checkMatch(matcher, "/a/b/c", "/a/b/c", "/a/b/", "/a");
    checkMatch(matcher, "/a/b/c/d", "/a/b/c", "/a/b/", "/a");
    checkMatch(matcher, "/a/bc", "/a/bc", "/a");
    checkMatch(matcher, "/a/bc/d", "/a/bc", "/a");
  }

  @Test
  public void matchWithNoLeadingSlash() {
    Set<String> paths = new HashSet<>();
    paths.add("/a/b");
    PrefixPathMatcher matcher = new PrefixPathMatcher(paths);
    checkNoMatch(matcher, "a");
    checkNoMatch(matcher, "a/b");
  }

  private void checkMatch(PrefixPathMatcher matcher, String path, String... expected) {
    Optional<List<String>> paths = matcher.match(new AlluxioURI(path));
    Assert.assertTrue(paths.isPresent());
    Assert.assertArrayEquals(expected, paths.get().toArray());
  }

  private void checkNoMatch(PrefixPathMatcher matcher, String path) {
    Assert.assertFalse(matcher.match(new AlluxioURI(path)).isPresent());
  }
}
