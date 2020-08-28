/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.grpc.WritePType;
import alluxio.grpc.TtlAction;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for setTtl command.
 */
public final class SetTtlCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void setTtl() throws Exception {
    String filePath = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL,
        sFileSystem.getStatus(new AlluxioURI("/testFile")).getTtl());

    AlluxioURI uri = new AlluxioURI("/testFile");
    long[] ttls = new long[] {0L, 1000L};
    for (long ttl : ttls) {
      Assert.assertEquals(0, sFsShell.run("setTtl", filePath, String.valueOf(ttl)));
      URIStatus status = sFileSystem.getStatus(uri);
      Assert.assertEquals(ttl, status.getTtl());
      Assert.assertEquals(TtlAction.DELETE, status.getTtlAction());
    }
  }

  @Test
  public void setTtlWithDelete() throws Exception {
    String filePath = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL,
        sFileSystem.getStatus(new AlluxioURI("/testFile")).getTtl());

    AlluxioURI uri = new AlluxioURI("/testFile");
    long ttl = 1000L;
    Assert.assertEquals(0,
        sFsShell.run("setTtl", "-action", "delete", filePath, String.valueOf(ttl)));
    URIStatus status = sFileSystem.getStatus(uri);
    Assert.assertEquals(ttl, status.getTtl());
    Assert.assertEquals(TtlAction.DELETE, status.getTtlAction());
  }

  @Test
  public void setTtlWithFree() throws Exception {
    String filePath = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL,
        sFileSystem.getStatus(new AlluxioURI("/testFile")).getTtl());

    AlluxioURI uri = new AlluxioURI("/testFile");
    long ttl = 1000L;
    Assert.assertEquals(0,
        sFsShell.run("setTtl", "-action", "free", filePath, String.valueOf(ttl)));
    URIStatus status = sFileSystem.getStatus(uri);
    Assert.assertEquals(ttl, status.getTtl());
    Assert.assertEquals(TtlAction.FREE, status.getTtlAction());
  }

  @Test
  public void setTtlSameTimeDifferentAction() throws Exception {
    String filePath = "/testFile";
    AlluxioURI uri = new AlluxioURI("/testFile");
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);
    long ttl = 1000L;

    Assert.assertEquals(0,
        sFsShell.run("setTtl", "-action", "delete", filePath, String.valueOf(ttl)));
    Assert.assertEquals(ttl, sFileSystem.getStatus(uri).getTtl());
    Assert.assertEquals(TtlAction.DELETE, sFileSystem.getStatus(uri).getTtlAction());

    Assert.assertEquals(0,
        sFsShell.run("setTtl", "-action", "free", filePath, String.valueOf(ttl)));
    Assert.assertEquals(ttl, sFileSystem.getStatus(uri).getTtl());
    Assert.assertEquals(TtlAction.FREE, sFileSystem.getStatus(uri).getTtlAction());
  }

  @Test
  public void setTtlWithNoOperationValue() throws Exception {
    String filePath = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL,
        sFileSystem.getStatus(new AlluxioURI("/testFile")).getTtl());

    long ttl = 1000L;
    Assert.assertEquals(-1, sFsShell.run("setTtl", "-action", filePath, String.valueOf(ttl)));
  }

  @Test
  public void setTtlWithInvalidOperationValue() throws Exception {
    String filePath = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL,
        sFileSystem.getStatus(new AlluxioURI("/testFile")).getTtl());

    long ttl = 1000L;
    Assert.assertEquals(-1,
        sFsShell.run("setTtl", "-action", "invalid", filePath, String.valueOf(ttl)));
  }

  @Test
  public void setTtlWithDifferentUnitTime() throws Exception {
    String filePath = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL,
        sFileSystem.getStatus(new AlluxioURI("/testFile")).getTtl());

    AlluxioURI uri = new AlluxioURI("/testFile");
    HashMap<String, Long> timeUnits = new HashMap<String, Long>();
    timeUnits.put("", 1L);
    timeUnits.put("ms", 1L);
    timeUnits.put("millisecond", 1L);
    timeUnits.put("s", (long) Constants.SECOND_MS);
    timeUnits.put("sec", (long) Constants.SECOND_MS);
    timeUnits.put("second", (long) Constants.SECOND_MS);
    timeUnits.put("m", (long) Constants.MINUTE_MS);
    timeUnits.put("min", (long) Constants.MINUTE_MS);
    timeUnits.put("minute", (long) Constants.MINUTE_MS);
    timeUnits.put("h", (long) Constants.HOUR_MS);
    timeUnits.put("hour", (long) Constants.HOUR_MS);
    timeUnits.put("d", (long) Constants.DAY_MS);
    timeUnits.put("day", (long) Constants.DAY_MS);
    long numericValue = 100;
    for (Map.Entry<String, Long> entry: timeUnits.entrySet()) {
      String timeUnit = entry.getKey();
      long timeUnitInMilliSeconds = entry.getValue();
      String testValueWithTimeUnit = String.valueOf(numericValue) + timeUnit;
      Assert.assertEquals(0, sFsShell.run("setTtl", filePath, testValueWithTimeUnit));
      URIStatus status = sFileSystem.getStatus(uri);
      Assert.assertEquals(numericValue * timeUnitInMilliSeconds, status.getTtl());
      Assert.assertEquals(TtlAction.DELETE, status.getTtlAction());
    }
  }

  @Test
  public void setTtlWithInvalidTime() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 1);
    sFsShell.run("setTtl", "/testFile", "some-random-text");
    Assert.assertTrue(mOutput.toString().contains("is not valid time"));
  }
}
