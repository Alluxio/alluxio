package tachyon.hadoop;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public final class UtilsTest {

  @Test
  public void testGetPathWithoutSchema() {
    final Path path = new Path("/foo/bar/baz");

    final String output = Utils.getPathWithoutScheme(path);
    Assert.assertEquals("/foo/bar/baz", output);
  }

  @Test
  public void testGetPathWithoutSchemaThatContainsSchema() {
    final Path path = new Path("file:///foo/bar/baz");

    final String output = Utils.getPathWithoutScheme(path);
    Assert.assertEquals("/foo/bar/baz", output);
  }

  /**
   * This test doesn't work the way you might expect.
   * 
   * If you take the URI.create("hdfs://localhost:1234/foo/bar/baz?please=dont&show=up").getPath it
   * will return /foo/bar/baz. If you go through Hadoop's Path using {@link Path#Path(String)} then
   * Hadoop injects the query params into the path, so when you call toURI it gives a different
   * response.
   */
  @Test
  public void testGetPathWithoutSchemaFromHDFS() {
    final Path path = new Path("hdfs://localhost:1234/foo/bar/baz?please=dont&show=up");

    final String output = Utils.getPathWithoutScheme(path);
    Assert.assertFalse("/foo/bar/baz".equals(output));
  }

  @Test
  public void testGetPathWithoutSchemaFromHDFSURI() {
    final Path path = new Path(URI.create("hdfs://localhost:1234/foo/bar/baz?please=dont&show=up"));

    final String output = Utils.getPathWithoutScheme(path);
    Assert.assertEquals("/foo/bar/baz", output);
  }
}
