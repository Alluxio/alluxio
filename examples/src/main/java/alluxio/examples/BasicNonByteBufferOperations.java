/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.Version;
import alluxio.client.AlluxioStorageType;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.AlluxioException;

/**
 * Basic example of using the {@link FileSystem} for writing to and reading from files.
 * <p>
 * This class is different from {@link alluxio.examples.BasicOperations} in the way writes happen.
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
  private final AlluxioURI mMasterLocation;
  private final AlluxioURI mFilePath;
  private final ReadType mReadType;
  private final WriteType mWriteType;
  private final boolean mDeleteIfExists;
  private final int mLength;

  /**
   * @param masterLocation the location of the master
   * @param filePath the path for the files
   * @param readType the {@link ReadType}
   * @param writeType the {@link WriteType}
   * @param deleteIfExists delete files if they already exist
   * @param length the number of files
   */
  public BasicNonByteBufferOperations(AlluxioURI masterLocation, AlluxioURI filePath, ReadType
      readType, WriteType writeType, boolean deleteIfExists, int length) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mWriteType = writeType;
    mReadType = readType;
    mDeleteIfExists = deleteIfExists;
    mLength = length;
  }

  @Override
  public Boolean call() throws Exception {
    ClientContext.getConf().set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost());
    ClientContext.getConf().set(Constants.MASTER_RPC_PORT,
        Integer.toString(mMasterLocation.getPort()));
    FileSystem tachyonClient = FileSystem.Factory.get();
    write(tachyonClient);
    return read(tachyonClient);
  }

  private void write(FileSystem tachyonClient) throws IOException, AlluxioException {
    FileOutStream fileOutStream = createFile(tachyonClient, mFilePath, mDeleteIfExists);
    DataOutputStream os = new DataOutputStream(fileOutStream);
    try {
      os.writeInt(mLength);
      for (int i = 0; i < mLength; i ++) {
        os.writeInt(i);
      }
    } finally {
      os.close();
    }
  }

  private FileOutStream createFile(FileSystem fileSystem, AlluxioURI filePath,
      boolean deleteIfExists) throws IOException, AlluxioException {
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(mWriteType);
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

  private boolean read(FileSystem tachyonClient) throws IOException, AlluxioException {
    OpenFileOptions options = OpenFileOptions.defaults().setReadType(mReadType);

    DataInputStream input = new DataInputStream(tachyonClient.openFile(mFilePath, options));
    try {
      int length = input.readInt();
      for (int i = 0; i < length; i ++) {
        if (input.readInt() != i) {
          return false;
        }
      }
    } finally {
      input.close();
    }
    return true;
  }

  /**
   * Usage: {@code java -cp <TACHYON-VERSION> BasicNonByteBufferOperations <master address>
   *   <file path> <ReadType (CACHE_PROMOTE | CACHE | NO_CACHE)>
   *   <WriteType (MUST_CACHE | CACHE_THROUGH | THROUGH)> <delete file> <number of files>}
   *
   * @param args the parameters to run the example
   * @throws IOException if the example fails to run
   */
  public static void main(final String[] args) throws IOException {
    if (args.length < 2 || args.length > 6) {
      usage();
    }

    Utils.runExample(new BasicNonByteBufferOperations(new AlluxioURI(args[0]), new AlluxioURI(
        args[1]), Utils.option(args, 2, ReadType.CACHE), Utils.option(args, 3,
        WriteType.CACHE_THROUGH), Utils.option(args, 4, true), Utils.option(args, 5, 20)));
  }

  private static void usage() {
    System.out.println("java -cp " + Version.TACHYON_JAR + " "
        + BasicNonByteBufferOperations.class.getName() + " <master address> <file path> "
        + " <ReadType (CACHE_PROMOTE | CACHE | NO_CACHE)> <WriteType (MUST_CACHE | CACHE_THROUGH"
        + " | THROUGH)> <delete file> <number of files>");
    System.exit(-1);
  }
}
