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
    testParentChild("hdfs://localhost:8080/a/b/d.txt", "hdfs://localhost:8080/a/b/c.txt",
        "../d.txt");
    testParentChild(".", ".", ".");
    testParentChild("/", "/", ".");
    testParentChild("/", ".", "/");
    testParentChild("/foo", "/", "foo");
    testParentChild("/foo/bar", "/foo", "bar");
    testParentChild("/foo/bar/baz", "/foo/bar", "baz");
    testParentChild("/foo/bar/baz", "/foo", "bar/baz");
    testParentChild("foo", ".", "foo");
    testParentChild("foo/bar", "foo", "bar");
    testParentChild("foo/bar/baz", "foo", "bar/baz");
    testParentChild("foo/bar/baz", "foo/bar", "baz");
    testParentChild("/foo", "/bar", "/foo");
    testParentChild("c:/foo", "/bar", "c:/foo");
    testParentChild("c:/foo", "d:/bar", "c:/foo");
    testParentChild("/foo/bar/baz/boo", "/foo/bar", "baz/boo");
    testParentChild("foo/bar/baz/bud", "foo/bar/", "baz/bud");
    testParentChild("/boo/bud", "/foo/bar", "../../boo/bud");
    testParentChild("boo/bud", "foo/bar", "../../boo/bud");
    testParentChild("boo/bud", ".", "boo/bud");
    testParentChild("/foo/boo/bud", "/foo/bar/baz", "../../boo/bud");
    testParentChild("foo/boo/bud", "foo/bar/baz", "../../boo/bud");
    testParentChild("../../../../boo/bud", "../../", "../../boo/bud");
    testParentChild("../../../../boo/bud", "../../foo", "../../../boo/bud");
    testParentChild("../../foo/boo/bud", "../../foo/bar", "../boo/bud");
    testParentChild("", "foo/bar/baz", "../../..");
    testParentChild("../..", "foo/bar/baz", "../../../../..");

    testParentChild("foo://bar boo:80/.", "foo://bar boo:80/.", ".");
    testParentChild("foo://bar boo:80/", "foo://bar boo:80/", ".");
    testParentChild("foo://bar boo:80/", "foo://bar boo:80/.", "/");
    testParentChild("foo://bar boo:80/foo", "foo://bar boo:80/", "foo");
    testParentChild("foo://bar boo:80/foo/bar", "foo://bar boo:80/foo", "bar");
    testParentChild("foo://bar boo:80/foo/bar/baz", "foo://bar boo:80/foo/bar", "baz");
    testParentChild("foo://bar boo:80/foo/bar/baz", "foo://bar boo:80/foo",
        "foo://bar boo:80/bar/baz");
    testParentChild("foo://bar boo:80/foo", "foo://bar boo:80/.", "foo");
    testParentChild("foo://bar boo:80/foo/bar/baz", "foo://bar boo:80/foo", "bar/baz");
    testParentChild("foo://bar boo:80/foo/bar/baz", "foo://bar boo:80/foo/bar", "baz");
    testParentChild("foo://bar boo:80/foo", "foo://bar boo:80/bar", "/foo");
    testParentChild("foo://bar boo:80/c:/foo", "foo://bar boo:80/bar", "c:/foo");
    testParentChild("foo://bar boo:80/c:/foo", "foo://bar boo:80/d:/bar", "c:/foo");
  }
}
