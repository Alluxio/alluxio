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

package alluxio.master.file.meta.cross.cluster;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Before;
import org.junit.Test;

public class CrossClusterIntersectionTest {

  private CrossClusterIntersection<String> mMappings;

  @Before
  public void before() {
    mMappings = new CrossClusterIntersection<>();
  }

  @Test
  public void checkIntersections() {
    mMappings.addMapping("c1", "s3://b1", "c1");
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1").toArray());
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2").toArray());
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b2").toArray());
    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1a").toArray());
    assertArrayEquals(new String[] {}, mMappings.getClusters("/s3").toArray());
    assertArrayEquals(new String[] {}, mMappings.getClusters("/s3/b1").toArray());

    mMappings.addMapping("c2", "s3://b1", "c2");
    assertArrayEquals(new String[] {"c1", "c2"}, mMappings.getClusters("s3://b1").toArray());
    assertArrayEquals(new String[] {"c1", "c2"}, mMappings.getClusters("s3://b1/b2").toArray());
    assertArrayEquals(new String[] {"c1", "c2"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.addMapping("c3", "s3://b1/b2", "c3");
    assertArrayEquals(new String[] {"c1", "c2"}, mMappings.getClusters("s3://b1").toArray());
    assertArrayEquals(new String[] {"c1", "c2", "c3"}, mMappings.getClusters(
        "s3://b1/b2").toArray());
    assertArrayEquals(new String[] {"c1", "c2", "c3"}, mMappings.getClusters(
        "s3://b1/b2/b3").toArray());
  }

  @Test
  public void checkDuplicateIntersections() {
    mMappings.addMapping("c1", "s3://b1", "c1");
    mMappings.addMapping("c1", "s3://b1", "c1");
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1").toArray());
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2").toArray());
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.addMapping("c1", "s3://b1/b2", "c1");
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1").toArray());
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2").toArray());
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.removeMapping("c1", "s3://b1", null);
    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1").toArray());
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2").toArray());
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.removeMapping("c1", "s3://b1/b2", null);
    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1").toArray());
    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1/b2").toArray());
    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1/b2/b3").toArray());
  }

  @Test
  public void checkRemoval() {
    mMappings.addMapping("c1", "s3://b1", "c1");
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.removeMapping("c1", "s3://b2", null);
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.removeMapping("c1", "s3://b1", null);
    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.addMapping("c1", "s3://b1", "c1");
    assertArrayEquals(new String[] {"c1"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.addMapping("c2", "s3://b1", "c2");
    assertArrayEquals(new String[] {"c1", "c2"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.removeMapping("c1", "s3://b1", null);
    assertArrayEquals(new String[] {"c2"}, mMappings.getClusters("s3://b1/b2/b3").toArray());

    mMappings.addMapping("c3", "s3://b1/b2", "c3");
    assertArrayEquals(new String[] {"c2", "c3"}, mMappings.getClusters(
        "s3://b1/b2/b3").toArray());

    mMappings.addMapping("c1", "s3://b1", "c1");
    assertArrayEquals(new String[] {"c1", "c2", "c3"}, mMappings.getClusters(
        "s3://b1/b2/b3").toArray());

    mMappings.removeMapping("c1", "s3://b1", null);
    mMappings.removeMapping("c2", "s3://b1", null);
    mMappings.removeMapping("c3", "s3://b1/b2", null);

    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1").toArray());
    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1/b2").toArray());
    assertArrayEquals(new String[] {}, mMappings.getClusters("s3://b1/b2/b3").toArray());
  }
}
