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

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.WritePType;
import alluxio.util.io.PathUtils;

import org.junit.Test;

import java.io.IOException;

/**
 * Tests for loadMetadata command.
 */
public final class LoadMetadataCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void loadMetadataDir() throws IOException, AlluxioException {
    String dirPath = "/testRoot/layer1/layer2/layer3/";
    String filePathA = PathUtils.concatPath(dirPath, "testFileA");
    String filePathB = PathUtils.concatPath(dirPath, "testFileB");
    FileSystemTestUtils
        .createByteFile(sFileSystem, filePathA, WritePType.CACHE_THROUGH, 10);
    FileSystemTestUtils
        .createByteFile(sFileSystem, filePathB, WritePType.CACHE_THROUGH, 30);
    AlluxioURI uriDir = new AlluxioURI(dirPath);
    AlluxioURI uriA = new AlluxioURI(filePathA);
    AlluxioURI uriB = new AlluxioURI(filePathB);
    URIStatus statusBeforeA = sFileSystem.getStatus(uriA);
    URIStatus statusBeforeB = sFileSystem.getStatus(uriB);
    // Delete layer3 directory metadata recursively.
    DeletePOptions deletePOptions =
        DeletePOptions.newBuilder().setAlluxioOnly(true).setRecursive(true).build();
    sFileSystem.delete(uriDir, deletePOptions);
    // Load metadata from ufs.
    sFsShell.run("loadMetadata", dirPath);
    // Use LoadMetadataPType.NEVER to avoid loading metadata during get file status.
    GetStatusPOptions getStatusPOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();
    // Check testFileA's metadata.
    URIStatus statusAfterA = sFileSystem.getStatus(uriA, getStatusPOptions);
    assertEquals(statusBeforeA.getFileInfo().getName(), statusAfterA.getFileInfo().getName());
    assertEquals(statusBeforeA.getFileInfo().getLength(), statusAfterA.getFileInfo().getLength());
    // Check testFileB's metadata.
    URIStatus statusAfterB = sFileSystem.getStatus(uriB, getStatusPOptions);
    assertEquals(statusBeforeB.getFileInfo().getName(), statusAfterB.getFileInfo().getName());
    assertEquals(statusBeforeB.getFileInfo().getLength(), statusAfterB.getFileInfo().getLength());
  }

  @Test
  public void loadMetadataFile() throws IOException, AlluxioException {
    String filePath = "/testRoot/layer1/layer2/layer3/testFile";
    FileSystemTestUtils
        .createByteFile(sFileSystem, filePath, WritePType.CACHE_THROUGH, 10);
    AlluxioURI uri = new AlluxioURI(filePath);
    URIStatus statusBefore = sFileSystem.getStatus(uri);
    DeletePOptions deletePOptions =
        DeletePOptions.newBuilder().setAlluxioOnly(true).setRecursive(true).build();
    sFileSystem.delete(uri, deletePOptions);
    sFsShell.run("loadMetadata", filePath);
    // Use LoadMetadataPType.NEVER to avoid loading metadata during get file status
    GetStatusPOptions getStatusPOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();
    URIStatus statusAfter = sFileSystem.getStatus(uri, getStatusPOptions);
    assertEquals(statusBefore.getFileInfo().getName(), statusAfter.getFileInfo().getName());
    assertEquals(statusBefore.getFileInfo().getLength(), statusAfter.getFileInfo().getLength());
  }

  @Test
  public void loadMetadataFileRecursive() throws IOException, AlluxioException {
    String filePath = "/testRoot/layer1/layer2/layer3/testFile";
    FileSystemTestUtils
        .createByteFile(sFileSystem, filePath, WritePType.CACHE_THROUGH, 10);
    AlluxioURI uri = new AlluxioURI(filePath);
    URIStatus statusBefore = sFileSystem.getStatus(uri);
    DeletePOptions deletePOptions =
        DeletePOptions.newBuilder().setAlluxioOnly(true).setRecursive(true).build();
    sFileSystem.delete(uri, deletePOptions);
    // Load metadata at /testRoot layer with -R option.
    sFsShell.run("loadMetadata", "-R", "/testRoot");
    // Use LoadMetadataPType.NEVER to avoid loading metadata during get file status
    GetStatusPOptions getStatusPOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();
    URIStatus statusAfter = sFileSystem.getStatus(uri, getStatusPOptions);
    assertEquals(statusBefore.getFileInfo().getName(), statusAfter.getFileInfo().getName());
    assertEquals(statusBefore.getFileInfo().getLength(), statusAfter.getFileInfo().getLength());
  }

  @Test
  public void loadMetadataFileWithWildcard() throws IOException, AlluxioException {
    String dirPath = "/testRoot/layer1/layer2/layer3/";
    String filePathA = PathUtils.concatPath(dirPath, "testFileA");
    String filePathB = PathUtils.concatPath(dirPath, "testFileB");
    FileSystemTestUtils
        .createByteFile(sFileSystem, filePathA, WritePType.CACHE_THROUGH, 10);
    FileSystemTestUtils
        .createByteFile(sFileSystem, filePathB, WritePType.THROUGH, 30);
    AlluxioURI uriA = new AlluxioURI(filePathA);
    AlluxioURI uriB = new AlluxioURI(filePathB);
    URIStatus statusBeforeA = sFileSystem.getStatus(uriA);
    URIStatus statusBeforeB = sFileSystem.getStatus(uriB);
    // Delete testFileA's metadata and testFileB's metadata.
    DeletePOptions deletePOptions =
        DeletePOptions.newBuilder().setAlluxioOnly(true).setRecursive(true).build();
    sFileSystem.delete(uriA, deletePOptions);
    sFileSystem.delete(uriB, deletePOptions);
    // Load metadata from ufs.
    sFsShell.run("loadMetadata", "/*/*/*/*/testFile*");
    // Use LoadMetadataPType.NEVER to avoid loading metadata during get file status.
    GetStatusPOptions getStatusPOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();
    // Check testFileA's metadata.
    URIStatus statusAfterA = sFileSystem.getStatus(uriA, getStatusPOptions);
    assertEquals(statusBeforeA.getFileInfo().getName(), statusAfterA.getFileInfo().getName());
    assertEquals(statusBeforeA.getFileInfo().getLength(), statusAfterA.getFileInfo().getLength());
    // Check testFileB's metadata.
    URIStatus statusAfterB = sFileSystem.getStatus(uriB, getStatusPOptions);
    assertEquals(statusBeforeB.getFileInfo().getName(), statusAfterB.getFileInfo().getName());
    assertEquals(statusBeforeB.getFileInfo().getLength(), statusAfterB.getFileInfo().getLength());
  }
}
