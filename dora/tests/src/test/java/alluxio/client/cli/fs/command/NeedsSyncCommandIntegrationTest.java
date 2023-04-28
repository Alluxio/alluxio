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
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.WritePType;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for needsSync command.
 */
public final class NeedsSyncCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void needsSyncDir() throws IOException, AlluxioException {
    CreateFilePOptions createOptions = CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.CACHE_THROUGH).build();
    AlluxioURI dir = new AlluxioURI("/dir");
    sFileSystem.createDirectory(dir);
    AlluxioURI file1 = dir.join("file1");
    sFileSystem.createFile(file1, createOptions).close();
    Assert.assertEquals(ImmutableList.of(file1.getPath()),
        sFileSystem.listStatus(dir).stream().map(URIStatus::getPath).collect(Collectors.toList()));
    // Create a file outside Alluxio
    AlluxioURI file2 = dir.join("file2");
    String ufsPath = Configuration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createFile(Paths.get(ufsPath, file2.getPath()));
    // It should not be synced since we already synced dir
    Assert.assertEquals(ImmutableList.of(file1.getPath()),
        sFileSystem.listStatus(dir).stream().map(URIStatus::getPath).collect(Collectors.toList()));
    // marking the path as needed sync should mean that the new file is synced on the next listing
    sFsShell.run("needsSync", dir.getPath());
    Assert.assertEquals(ImmutableList.of(file1.getPath(), file2.getPath()),
        sFileSystem.listStatus(dir).stream().map(URIStatus::getPath).collect(Collectors.toList()));
  }

  @Test
  public void needsSyncFile() throws IOException, AlluxioException {
    CreateFilePOptions createOptions = CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.CACHE_THROUGH).build();
    AlluxioURI dir = new AlluxioURI("/dir");
    sFileSystem.createDirectory(dir);
    AlluxioURI file1 = dir.join("file1");
    String fileContents = "some bytes";
    try (FileOutStream f = sFileSystem.createFile(file1, createOptions)) {
      f.write(fileContents.getBytes());
    }
    Assert.assertEquals(fileContents, new String(IOUtils.toByteArray(sFileSystem.openFile(file1))));

    // modify the file outside Alluxio
    String newFileContents = "a new file write that is longer";
    String ufsPath = Configuration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.write(Paths.get(ufsPath, file1.getPath()), newFileContents.getBytes());

    // we should not see the updated file
    Assert.assertEquals(fileContents, new String(IOUtils.toByteArray(sFileSystem.openFile(file1))));
    // marking the path as needed sync should mean that the new file is synced on the next listing
    sFsShell.run("needsSync", file1.getPath());
    Assert.assertEquals(newFileContents, new String(IOUtils.toByteArray(
        sFileSystem.openFile(file1))));
  }

  @Test
  public void needsSyncWithWildcard() throws IOException, AlluxioException {
    CreateFilePOptions createOptions = CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.CACHE_THROUGH).build();
    ListStatusPOptions listStatusOptions = ListStatusPOptions.newBuilder()
        .setRecursive(true).build();
    CreateDirectoryPOptions createDirectoryOptions = CreateDirectoryPOptions.newBuilder()
        .setRecursive(true).build();

    // Create a directory with a file outside alluxio
    AlluxioURI dir2 = new AlluxioURI("/dir1/dir2");
    sFileSystem.createDirectory(dir2, createDirectoryOptions);
    AlluxioURI file1 = dir2.join("file1");
    sFileSystem.createFile(file1, createOptions).close();
    List<String> listing = ImmutableList.of(file1.getPath());
    Assert.assertEquals(listing, sFileSystem.listStatus(dir2, listStatusOptions).stream()
            .map(URIStatus::getPath).collect(Collectors.toList()));
    // Create a file outside Alluxio
    AlluxioURI file2 = dir2.join("file2");
    String ufsPath = Configuration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createFile(Paths.get(ufsPath, file2.getPath()));
    Assert.assertEquals(listing, sFileSystem.listStatus(dir2, listStatusOptions).stream()
        .map(URIStatus::getPath).collect(Collectors.toList()));

    // Repeat the process for a separate path
    AlluxioURI dir3 = new AlluxioURI("/dir1/dir3");
    sFileSystem.createDirectory(dir3, createDirectoryOptions);
    AlluxioURI file3 = dir3.join("file3");
    sFileSystem.createFile(file3, createOptions).close();
    listing = ImmutableList.of(file3.getPath());
    Assert.assertEquals(listing, sFileSystem.listStatus(dir3, listStatusOptions).stream()
        .map(URIStatus::getPath).collect(Collectors.toList()));
    // Create a file outside Alluxio
    AlluxioURI file4 = dir3.join("file4");
    Files.createFile(Paths.get(ufsPath, file4.getPath()));
    Assert.assertEquals(listing, sFileSystem.listStatus(dir3, listStatusOptions).stream()
        .map(URIStatus::getPath).collect(Collectors.toList()));

    // marking the path as needed sync should mean that the new file is synced on the next listing
    sFsShell.run("needsSync", "/dir*/*/file*");
    Assert.assertEquals(ImmutableList.of(file1.getPath(), file2.getPath()),
        sFileSystem.listStatus(dir2, listStatusOptions).stream().map(URIStatus::getPath)
            .collect(Collectors.toList()));
    Assert.assertEquals(ImmutableList.of(file3.getPath(), file4.getPath()),
        sFileSystem.listStatus(dir3, listStatusOptions).stream().map(URIStatus::getPath)
            .collect(Collectors.toList()));
  }
}
