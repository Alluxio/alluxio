package tachyon;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link UnderFileSystem}
 */
public final class UnderFileSystemTest {

  @Test
  public void parseTest() {
    Pair<String, String> result = UnderFileSystem.parse("/path");
    Assert.assertEquals(result.getFirst(), "/");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse("file:///path");
    Assert.assertEquals(result.getFirst(), "/");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse("tachyon://localhost:19998");
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse("tachyon://localhost:19998/");
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse("tachyon://localhost:19998/path");
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse("tachyon-ft://localhost:19998/path");
    Assert.assertEquals(result.getFirst(), "tachyon-ft://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse("hdfs://localhost:19998/path");
    Assert.assertEquals(result.getFirst(), "hdfs://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse("s3://localhost:19998/path");
    Assert.assertEquals(result.getFirst(), "s3://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse("s3n://localhost:19998/path");
    Assert.assertEquals(result.getFirst(), "s3n://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    Assert.assertEquals(UnderFileSystem.parse(null), null);
    Assert.assertEquals(UnderFileSystem.parse(""), null);
    Assert.assertEquals(UnderFileSystem.parse("anythingElse"), null);
  }
}
