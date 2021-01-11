package alluxio.proxy.s3;

import alluxio.client.file.FileSystem;

import javax.security.auth.Subject;

public class S3RestServiceHelper {

  private S3RestServiceHelper() {}

  public static FileSystem getFileSystem(Subject subject) {
    return FileSystem.Factory.get(subject);
  }

}
