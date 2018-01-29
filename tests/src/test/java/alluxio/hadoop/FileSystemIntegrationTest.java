package alluxio.hadoop;

import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.security.authentication.AuthType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;

/**
 * Integration tests for {@link FileSystem}.
 */
public class FileSystemIntegrationTest {
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName())
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true")
          .build();

  private static org.apache.hadoop.fs.FileSystem sTFS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());

    URI uri = URI.create(sLocalAlluxioClusterResource.get().getMasterURI());

    sTFS = org.apache.hadoop.fs.FileSystem.get(uri, conf);
  }

  @Test
  public void closeFileSystem() throws Exception {
    Path file = new Path("/createfile");
    FsPermission permission = FsPermission.createImmutable((short) 0666);
    FSDataOutputStream o = sTFS.create(file, permission, false /* ignored */, 10 /* ignored */,
        (short) 1 /* ignored */, 512 /* ignored */, null /* ignored */);
    o.writeBytes("Test Bytes");
    o.close();
    // with mark of delete-on-exit, the close method will try to delete it
    sTFS.deleteOnExit(file);
    sTFS.close();
  }
}
