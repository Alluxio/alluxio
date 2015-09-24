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

package tachyon.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.ClientContext;
import tachyon.client.ClientOptions;
import tachyon.client.TachyonStorageType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Basic example of using the TachyonFS and TachyonFile for writing to and reading from files.
 * <p>
 * This class is different from {@link tachyon.examples.BasicOperations} in the way writes happen.
 * Over there {@link java.nio.ByteBuffer} is used directly, where as here byte data is done via
 * input/output streams.
 * </p>
 * <p>
 * This example also let users play around with how to work with files a bit more. The
 * {@link tachyon.client.TachyonStorageType} is something that can be set, as well as ability to
 * delete file if exists.
 * </p>
 */
public final class BasicNonByteBufferOperations implements Callable<Boolean> {
  // private access to the reinitializer of ClientContext
  private static ClientContext.ReinitializerAccesser sReinitializerAccesser =
      new ClientContext.ReinitializerAccesser() {
        @Override
        public void receiveAccess(ClientContext.PrivateReinitializer access) {
          sReinitializer = access;
        }
      };
  private static ClientContext.PrivateReinitializer sReinitializer;
  private final TachyonURI mMasterLocation;
  private final TachyonURI mFilePath;
  private final TachyonStorageType mWriteType;
  private final TachyonStorageType mReadType;
  private final boolean mDeleteIfExists;
  private final int mLength;

  public BasicNonByteBufferOperations(TachyonURI masterLocation, TachyonURI filePath,
      TachyonStorageType writeType, TachyonStorageType readType, boolean deleteIfExists,
      int length) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mWriteType = writeType;
    mReadType = readType;
    mDeleteIfExists = deleteIfExists;
    mLength = length;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonConf tachyonConf = ClientContext.getConf();
    tachyonConf.set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost());
    tachyonConf.set(Constants.MASTER_PORT, Integer.toString(mMasterLocation.getPort()));
    if (sReinitializer == null) {
      ClientContext.accessReinitializer(sReinitializerAccesser);
    }
    sReinitializer.reinitializeWithConf(tachyonConf);
    TachyonFileSystem tFS = TachyonFileSystem.get();
    write(tFS, mFilePath, mWriteType, mDeleteIfExists, mLength);
    return read(tFS, mFilePath, mReadType);
  }

  private void write(TachyonFileSystem tachyonFileSystem, TachyonURI filePath,
      TachyonStorageType writeType, boolean deleteIfExists, int length)
          throws IOException, BlockInfoException, FileDoesNotExistException, InvalidPathException,
          FileAlreadyExistException {
    ClientOptions clientOptions =
        new ClientOptions.Builder(ClientContext.getConf()).setTachyonStoreType(writeType).build();
    // If the file exists already, we will override it.
    FileOutStream fileOutStream =
        getOrCreate(tachyonFileSystem, filePath, deleteIfExists, clientOptions);
    DataOutputStream os = new DataOutputStream(fileOutStream);
    try {
      os.writeInt(length);
      for (int i = 0; i < length; i ++) {
        os.writeInt(i);
      }
    } finally {
      os.close();
    }
  }

  private FileOutStream getOrCreate(TachyonFileSystem tachyonFileSystem, TachyonURI filePath,
      boolean deleteIfExists, ClientOptions clientOptions) throws IOException, BlockInfoException,
          FileAlreadyExistException, InvalidPathException, FileDoesNotExistException {
    TachyonFile file;

    try {
      file = tachyonFileSystem.open(filePath);
    } catch (Exception e) {
      file = null;
    }
    if (file == null) {
      // file doesn't exist yet, so create it
      return tachyonFileSystem.getOutStream(filePath, clientOptions);
    } else if (deleteIfExists) {
      // file exists, so delete it and recreate
      tachyonFileSystem.delete(file);
      return tachyonFileSystem.getOutStream(filePath, clientOptions);
    }
    return null;
  }

  private boolean read(TachyonFileSystem tachyonFileSystem, TachyonURI filePath,
      TachyonStorageType readType)
          throws IOException, InvalidPathException, FileDoesNotExistException {
    ClientOptions clientOptions =
        new ClientOptions.Builder(ClientContext.getConf()).setTachyonStoreType(readType).build();
    TachyonFile file = tachyonFileSystem.open(filePath);
    DataInputStream input = new DataInputStream(tachyonFileSystem.getInStream(file, clientOptions));
    boolean passes = true;
    try {
      int length = input.readInt();
      for (int i = 0; i < length; i ++) {
        passes &= (input.readInt() == i);
      }
    } finally {
      input.close();
    }
    return passes;
  }

  public static void main(final String[] args) throws IOException {
    if (args.length < 2 || args.length > 6) {
      usage();
    }

    Utils.runExample(new BasicNonByteBufferOperations(new TachyonURI(args[0]),
        new TachyonURI(args[1]), Utils.option(args, 2, TachyonStorageType.STORE),
        Utils.option(args, 3, TachyonStorageType.NO_STORE), Utils.option(args, 4, true),
        Utils.option(args, 5, 20)));
  }

  private static void usage() {
    System.out.println("java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
        + BasicNonByteBufferOperations.class.getName() + " <TachyonMasterAddress> <FilePath> "
        + "[WriteType(STORE|NO_STORE)] [ReadType(STORE|NO_STORE)] [delete file] [num writes]");
    System.exit(-1);
  }
}
