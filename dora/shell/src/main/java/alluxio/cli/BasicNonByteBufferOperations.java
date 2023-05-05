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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.client.AlluxioStorageType;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Basic example of using the {@link FileSystem} for writing to and reading from files.
 * <p>
 * This class is different from {@link BasicOperations} in the way writes happen.
 * Over there {@link java.nio.ByteBuffer} is used directly, where as here byte data is done via
 * input/output streams.
 * </p>
 * <p>
 * This example also let users play around with how to work with files a bit more. The
 * {@link AlluxioStorageType} is something that can be set, as well as ability to
 * delete file if exists.
 * </p>
 */
public final class BasicNonByteBufferOperations implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(BasicNonByteBufferOperations.class);

  private final AlluxioURI mFilePath;
  private final ReadType mReadType;
  private final WriteType mWriteType;
  private final boolean mDeleteIfExists;
  private final int mLength;
  private final FileSystemContext mFsContext;

  /**
   * @param filePath the path for the files
   * @param readType the {@link ReadType}
   * @param writeType the {@link WriteType}
   * @param deleteIfExists delete files if they already exist
   * @param length the number of files
   * @param fsContext the {@link FileSystemContext} to use for client operations
   */
  public BasicNonByteBufferOperations(AlluxioURI filePath, ReadType readType, WriteType writeType,
      boolean deleteIfExists, int length, FileSystemContext fsContext) {
    mFilePath = filePath;
    mWriteType = writeType;
    mReadType = readType;
    mDeleteIfExists = deleteIfExists;
    mLength = length;
    mFsContext = fsContext;
  }

  @Override
  public Boolean call() throws Exception {
    FileSystem alluxioClient = FileSystem.Factory.create(mFsContext);
    write(alluxioClient);
    return read(alluxioClient);
  }

  private void write(FileSystem alluxioClient) throws IOException, AlluxioException {
    FileOutStream fileOutStream = createFile(alluxioClient, mFilePath, mDeleteIfExists);
    long startTimeMs = CommonUtils.getCurrentMs();
    try (DataOutputStream os = new DataOutputStream(fileOutStream)) {
      os.writeInt(mLength);
      for (int i = 0; i < mLength; i++) {
        os.writeInt(i);
      }
    }
    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "writeFile to file " + mFilePath));
  }

  private FileOutStream createFile(FileSystem fileSystem, AlluxioURI filePath,
      boolean deleteIfExists) throws IOException, AlluxioException {
    CreateFilePOptions options = CreateFilePOptions.newBuilder().setWriteType(mWriteType.toProto())
        .setRecursive(true).build();
    if (!fileSystem.exists(filePath)) {
      // file doesn't exist yet, so create it
      return fileSystem.createFile(filePath, options);
    } else if (deleteIfExists) {
      // file exists, so delete it and recreate
      fileSystem.delete(filePath);
      return fileSystem.createFile(filePath, options);
    }
    // file exists and deleteIfExists is false
    throw new FileAlreadyExistsException("File exists and deleteIfExists is false");
  }

  private boolean read(FileSystem alluxioClient) throws IOException, AlluxioException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(mReadType.toProto()).build();
    boolean pass = true;
    long startTimeMs = CommonUtils.getCurrentMs();
    try (DataInputStream input = new DataInputStream(alluxioClient.openFile(mFilePath, options))) {
      int length = input.readInt();
      for (int i = 0; i < length; i++) {
        if (input.readInt() != i) {
          pass = false;
          break;
        }
      }
    }
    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "readFile file " + mFilePath));
    return pass;
  }
}
