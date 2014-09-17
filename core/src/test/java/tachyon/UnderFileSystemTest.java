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

    Assert.assertEquals(UnderFileSystem.parse((String) null), null);
    Assert.assertEquals(UnderFileSystem.parse(""), null);
    Assert.assertEquals(UnderFileSystem.parse("anythingElse"), null);
  }

  @Test
  public void parseTest2() {
    Pair<String, String> result = UnderFileSystem.parse(new TachyonURI("/path"));
    Assert.assertEquals(result.getFirst(), "/");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("file:///path"));
    Assert.assertEquals(result.getFirst(), "/");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("tachyon://localhost:19998"));
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new TachyonURI("tachyon://localhost:19998/"));
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new TachyonURI("tachyon://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("tachyon-ft://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "tachyon-ft://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("hdfs://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "hdfs://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("s3://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "s3://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("s3n://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "s3n://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    Assert.assertEquals(UnderFileSystem.parse((TachyonURI) null), null);
    Assert.assertEquals(UnderFileSystem.parse(TachyonURI.EMPTY_URI), null);
    Assert.assertEquals(UnderFileSystem.parse(new TachyonURI("anythingElse")), null);
  }
}
