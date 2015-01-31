package tachyon.underfs.hdfs;

import org.junit.Assert;
import org.junit.Test;

import tachyon.underfs.UnderFileSystemFactory;
import tachyon.underfs.UnderFileSystemRegistry;

public class HdfsUnderFileSystemTest {

  @Test
  public void factoryTest() {
    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("hdfs://localhost/test/path");
    Assert.assertNotNull("A UnderFileSystemFactory should exist for HDFS paths when using this module", factory);
    
    factory = UnderFileSystemRegistry.find("s3://localhost/test/path");
    Assert.assertNull("A UnderFileSystemFactory should exist for S3 paths when using this module", factory);
    
    factory = UnderFileSystemRegistry.find("s3n://localhost/test/path");
    Assert.assertNull("A UnderFileSystemFactory should exist for S3 paths when using this module", factory);
  }
}
