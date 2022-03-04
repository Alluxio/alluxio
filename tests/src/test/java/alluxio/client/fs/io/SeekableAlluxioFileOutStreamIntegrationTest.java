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

package alluxio.client.fs.io;

import static alluxio.client.WriteType.THROUGH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.SeekableAlluxioFileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.grpc.CreateFilePOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

/**
 * Integration tests for {@code SeekableAlluxioFileOutStream}.
 */
public class SeekableAlluxioFileOutStreamIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {

  private final AlluxioURI mAlluxioPath = new AlluxioURI(PathUtils.uniqPath());
  private final CreateFilePOptions mOptions = CreateFilePOptions.newBuilder()
      .setWriteType(THROUGH.toProto())
      .setSeekable(true)
      .setRecursive(true)
      .build();

  @Test
  public void createSeekableAlluxioFileOutStream() throws Exception {
    // write type must be THROUGH
    for (WriteType type : WriteType.values()) {
      if (type == THROUGH) {
        continue;
      }
      AlluxioURI path = new AlluxioURI(PathUtils.uniqPath() + type.toString());
      CreateFilePOptions options = mOptions.toBuilder()
          .setSeekable(true).setWriteType(type.toProto()).build();
      try (FileOutStream os = mFileSystem.createFile(path, options)) {
        assertFalse(os instanceof SeekableAlluxioFileOutStream);
      }
    }
  }

  @Test
  public void createSeekableAlluxioFileOutStream2() throws Exception {
    // seekable must be true
    for (WriteType type : WriteType.values()) {
      AlluxioURI path = new AlluxioURI(PathUtils.uniqPath() + type.toString());
      CreateFilePOptions options =
          mOptions.toBuilder().setSeekable(false).setWriteType(type.toProto()).build();
      try (FileOutStream os = mFileSystem.createFile(path, options)) {
        assertFalse(os instanceof SeekableAlluxioFileOutStream);
      }
    }
  }

  @Test
  public void createEmptyFile() throws Exception {
    try (SeekableAlluxioFileOutStream os =
        (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
    }
    checkFileInAlluxio(mAlluxioPath, 0);
    checkFileInUnderStorage(mAlluxioPath, 0);
  }

  @Test
  public void createEmptyFile2() throws Exception {
    try (SeekableAlluxioFileOutStream os =
        (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      os.write((byte) 0);
      os.seek(0);
    }
    checkFileInAlluxio(mAlluxioPath, 1);
    checkFileInUnderStorage(mAlluxioPath, 1);
  }

  @Test
  public void writeAndSeek() throws Exception {
    try (SeekableAlluxioFileOutStream os =
        (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      os.write((byte) 0);
      os.write((byte) 1);
      os.seek(1);
    }
    checkFileInAlluxio(mAlluxioPath, 2);
    checkFileInUnderStorage(mAlluxioPath, 2);
  }

  @Test
  public void writeAndSeek2() throws Exception {
    try (SeekableAlluxioFileOutStream os =
        (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      os.write((byte) 0);
      os.write((byte) 1);
      os.write((byte) 2);
      os.seek(1);
      os.write((byte) 1);
      os.write((byte) 2);
    }
    checkFileInAlluxio(mAlluxioPath, 3);
    checkFileInUnderStorage(mAlluxioPath, 3);
  }

  @Test
  public void writeAndSeek3() throws Exception {
    try (SeekableAlluxioFileOutStream os =
             (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      os.write(BufferUtils.getIncreasingByteArray(0, 1024));
      os.seek(0);
      os.write(BufferUtils.getIncreasingByteArray(0, 1024));
    }
    checkFileInAlluxio(mAlluxioPath, 1024);
    checkFileInUnderStorage(mAlluxioPath, 1024);
  }

  @Test
  public void writeAndSeek4() throws Exception {
    try (SeekableAlluxioFileOutStream os =
             (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      os.write(BufferUtils.getIncreasingByteArray(0, 1000));
      os.seek(500);
      os.write(BufferUtils.getIncreasingByteArray(500, 1500));
    }
    checkFileInAlluxio(mAlluxioPath, 2000);
    checkFileInUnderStorage(mAlluxioPath, 2000);
  }

  @Test
  public void overwriteAfterClose() throws Exception {
    try (SeekableAlluxioFileOutStream os =
        (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      os.write((byte) 3);
      os.seek(0);
      os.write((byte) 0);
      os.write((byte) 1);
    }
    checkFileInAlluxio(mAlluxioPath, 2);
    checkFileInUnderStorage(mAlluxioPath, 2);
    mFileSystem.delete(mAlluxioPath);
    assertFalse(mFileSystem.exists(mAlluxioPath));
    try (SeekableAlluxioFileOutStream os =
        (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      os.write((byte) 0);
      os.write((byte) 2);
      os.seek(1);
      os.write((byte) 1);
      os.write((byte) 2);
    }
    checkFileInAlluxio(mAlluxioPath, 3);
    checkFileInUnderStorage(mAlluxioPath, 3);
  }

  @Test
  public void reopenAfterClose() throws Exception {
    try (SeekableAlluxioFileOutStream os =
             (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      os.write(BufferUtils.getIncreasingByteArray(0, 1000));
    }
    checkFileInAlluxio(mAlluxioPath, 1000);
    checkFileInUnderStorage(mAlluxioPath, 1000);
    URIStatus status = mFileSystem.getStatus(mAlluxioPath);
    try (SeekableAlluxioFileOutStream os =
        SeekableAlluxioFileOutStream.open(mAlluxioPath, status, mFileSystem)) {
      os.seek(500);
      os.write(BufferUtils.getIncreasingByteArray(500, 2000));
    }
    checkFileInAlluxio(mAlluxioPath, 2500);
    checkFileInUnderStorage(mAlluxioPath, 2500);
  }

  @Test
  public void getPosAndBytesWritten() throws Exception {
    try (SeekableAlluxioFileOutStream os =
        (SeekableAlluxioFileOutStream) mFileSystem.createFile(mAlluxioPath, mOptions)) {
      assertEquals(0, os.getPos());
      assertEquals(0, os.getBytesWritten());
      os.write((byte) 0);
      assertEquals(1, os.getPos());
      assertEquals(1, os.getBytesWritten());
      os.seek(0);
      assertEquals(0, os.getPos());
      assertEquals(1, os.getBytesWritten());
      os.write((byte) 0);
      assertEquals(1, os.getPos());
      assertEquals(1, os.getBytesWritten());
      os.seek(1024);
      assertEquals(1024, os.getPos());
      assertEquals(1024, os.getBytesWritten());
      os.write((byte) 0);
      assertEquals(1025, os.getPos());
      assertEquals(1025, os.getBytesWritten());
    }
  }
}
