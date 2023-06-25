package alluxio.worker.http;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestHttpServerHandler {

  @Test
  public void testGetRequestMapping() {
    String requestUri = "http://localhost:28080/page?"
        + "fileId=762445dac40c26f1d53d9e97e8984f7ab1ac4059618fa44aa73a485c63a1ff45&pageIndex=0";
    String requestMapping = getRequestMapping(requestUri);
    Assert.assertEquals( "page", requestMapping);
  }

  private String getRequestMapping(String requestUri) {
    int endIndex = requestUri.indexOf("?");
    int startIndex = requestUri.lastIndexOf("/", endIndex);
    String requestMapping = requestUri.substring(startIndex + 1, endIndex);
    return requestMapping;
  }

  @Test
  public void testCreateFileSystemContext() {
    FileSystemContext.FileSystemContextFactory factory =
        new FileSystemContext.FileSystemContextFactory();
    FileSystemContext fileSystemContext = factory.create(Configuration.global());
  }

}
