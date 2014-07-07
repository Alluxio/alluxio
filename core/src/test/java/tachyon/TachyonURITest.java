package tachyon;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Unit tests for tachyon.TachyonURITest
 */
public class TachyonURITest {
  @Test
  public void basicTest() {
    TachyonURI uri = new TachyonURI("tachyon://localhost:19998/xy z/a b c");
    Assert.assertEquals("localhost:19998", uri.getAuthority());
    Assert.assertEquals(2, uri.getDepth());
    Assert.assertEquals("localhost", uri.getHost());
    Assert.assertEquals("a b c", uri.getName());
    Assert.assertEquals("tachyon://localhost:19998/xy z", uri.getParent().toString());
    Assert.assertEquals("tachyon://localhost:19998/", uri.getParent().getParent().toString());
    Assert.assertEquals("/xy z/a b c", uri.getPath());
    Assert.assertEquals(19998, uri.getPort());
    Assert.assertEquals("tachyon", uri.getScheme());
    Assert.assertEquals(true, uri.hasAuthority());
    Assert.assertEquals(true, uri.hasScheme());
    Assert.assertEquals(true, uri.isAbsolute());
    Assert.assertEquals(true, uri.isPathAbsolute());
    Assert.assertEquals("tachyon://localhost:19998/xy z/a b c/d", uri.join("/d").toString());
    Assert.assertEquals("tachyon://localhost:19998/xy z/a b c/d", uri.join(new TachyonURI("/d"))
        .toString());
    Assert.assertEquals("tachyon://localhost:19998/xy z/a b c", uri.toString());
  }

  @Test
  public void basicTests() {
    String[] strs =
        new String[] { "tachyon://localhost:19998/xyz/abc", "hdfs://localhost:19998/xyz/abc",
            "s3://localhost:19998/xyz/abc", "tachyon://localhost:19998/xy z/a b c",
            "hdfs://localhost:19998/xy z/a b c", "s3://localhost:19998/xy z/a b c" };
    for (String str : strs) {
      TachyonURI uri = new TachyonURI(str);
      Assert.assertEquals(str, uri.toString());
      Assert.assertEquals(2, uri.getDepth());
      Assert.assertEquals("localhost", uri.getHost());
      Assert.assertEquals(19998, uri.getPort());
    }
  }

  private void testParentChild(String target, String parent, String child) {
    Assert.assertEquals(new TachyonURI(target), new TachyonURI(new TachyonURI(parent),
        new TachyonURI(child)));
  }

  @Test
  public void constructFromParentAndChildTests() {
    testParentChild(".", ".", ".");
    testParentChild("/", "/", ".");
    testParentChild("/", ".", "/");
  }
}
