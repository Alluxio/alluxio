package alluxio.master.file.meta.crosscluster;

import static org.junit.Assert.assertArrayEquals;

import alluxio.master.file.meta.crosscluster.CrossClusterIntersection;
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
