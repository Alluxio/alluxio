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

/**
 * Tests {@link PathIndexer}.
 */
public class PathIndexerTest {
  @Test
  public void index() {
    String[] paths = new String[]{
        "/a",
        "/a/b/",
        "/a/b/c",
        "/a/bc/d",
    };
    AlluxioProperties properties = new AlluxioProperties();
    properties.set(PropertyKey.PATHS, String.format("%d", paths.length));
    for (int i = 0; i < paths.length; i++) {
      properties.set(PropertyKey.Template.PATH_INDEX.format(i), paths[i]);
    }
    AlluxioConfiguration conf = new InstancedConfiguration(properties);
    PathIndexer indexer = new PathIndexer(conf);
    Assert.assertEquals(PathIndexer.NO_INDEX, indexer.index("/"));
    Assert.assertEquals(PathIndexer.NO_INDEX, indexer.index("/ab"));
    Assert.assertEquals(PathIndexer.NO_INDEX, indexer.index("/b/c"));
    Assert.assertEquals(0, indexer.index("/a"));
    Assert.assertEquals(0, indexer.index("/a/c"));
    Assert.assertEquals(1, indexer.index("/a/b"));
    Assert.assertEquals(1, indexer.index("/a/b/d"));
    Assert.assertEquals(2, indexer.index("/a/b/c"));
    Assert.assertEquals(2, indexer.index("/a/b/c/d"));
    Assert.assertEquals(3, indexer.index("/a/bc/d"));
    Assert.assertEquals(3, indexer.index("/a/bc/d/ef/g/hijk"));
  }
}
