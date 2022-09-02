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

package alluxio.client.fuse.file;

import alluxio.AlluxioURI;
import alluxio.fuse.file.FuseFileStream;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * An integration test for mixing different fuse file streams.
 */
public class MixedFuseStreamIntegrationTest extends AbstractFuseFileStreamIntegrationTest {
  @Test
  public void writeThenOpenRead() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    FuseFileStream outStream = mStreamFactory.create(
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE);
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
    outStream.write(buffer, DEFAULT_FILE_LEN, 0);
    Thread thread = new Thread(() -> {
      try (FuseFileStream inStream = mStreamFactory
          .create(alluxioURI, OpenFlags.O_RDONLY.intValue(), MODE)) {
        ByteBuffer readBuffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN, inStream.read(readBuffer, DEFAULT_FILE_LEN, 0));
        Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, DEFAULT_FILE_LEN, readBuffer));
      }
    });
    thread.start();
    // Fuse.release() is async. Out stream may be closed after the file is opened for read
    outStream.close();
    thread.join();
  }

  @Test
  public void writeThenOpenReadWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    FuseFileStream outStream = mStreamFactory.create(
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE);
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
    outStream.write(buffer, DEFAULT_FILE_LEN, 0);
    Thread thread = new Thread(() -> {
      try (FuseFileStream inOrOutStream = mStreamFactory
          .create(alluxioURI, OpenFlags.O_RDWR.intValue(), MODE)) {
        ByteBuffer readBuffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN, inOrOutStream.read(readBuffer, DEFAULT_FILE_LEN, 0));
        Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, DEFAULT_FILE_LEN, readBuffer));
      }
    });
    thread.start();
    // Fuse.release() is async. Out stream may be closed after the file is opened for read/write
    outStream.close();
    thread.join();
  }

  @Test
  public void writeThenOverwrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    FuseFileStream outStream = mStreamFactory.create(
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE);
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
    outStream.write(buffer, DEFAULT_FILE_LEN, 0);
    Thread thread = new Thread(() -> {
      try (FuseFileStream overwriteStream = mStreamFactory.create(alluxioURI,
          OpenFlags.O_WRONLY.intValue() | OpenFlags.O_TRUNC.intValue(), MODE)) {
        ByteBuffer newBuffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        overwriteStream.write(newBuffer, DEFAULT_FILE_LEN, 0);
        newBuffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
        overwriteStream.write(newBuffer, DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      }
    });
    // Fuse.release() is async. Out stream may be closed after the file is opened for overwrite
    thread.start();
    outStream.close();
    thread.join();
    checkFileInAlluxio(alluxioURI, 2 * DEFAULT_FILE_LEN, 0);
  }

  @Test
  public void writeThenOverwriteReadWrite() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(alluxioURI.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    FuseFileStream outStream = mStreamFactory.create(
        alluxioURI, OpenFlags.O_WRONLY.intValue(), MODE);
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
    outStream.write(buffer, DEFAULT_FILE_LEN, 0);
    Thread thread = new Thread(() -> {
      try (FuseFileStream inOrOutStream = mStreamFactory.create(alluxioURI,
          OpenFlags.O_RDWR.intValue() | OpenFlags.O_TRUNC.intValue(), MODE)) {
        ByteBuffer newBuffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        inOrOutStream.write(newBuffer, DEFAULT_FILE_LEN, 0);
        newBuffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
        inOrOutStream.write(newBuffer, DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
      }
    });
    thread.start();
    // Fuse.release() is async. Out stream may be closed after the file is opened for read/write
    outStream.close();
    thread.join();
    checkFileInAlluxio(alluxioURI, 2 * DEFAULT_FILE_LEN, 0);
  }
}
