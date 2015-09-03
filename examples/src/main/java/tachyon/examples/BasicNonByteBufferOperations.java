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

import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;

/**
 * Basic example of using the TachyonFS and TachyonFile for writing to and reading from files.
 * <p>
 * This class is different from {@link tachyon.examples.BasicOperations} in the way writes happen.
 * Over there {@link java.nio.ByteBuffer} is used directly, where as here byte data is done via
 * input/output streams.
 * </p>
 * <p>
 * This example also let users play around with how to work with files a bit more. The
 * {@link tachyon.client.ReadType} is something that can be set, as well as ability to delete file
 * if exists.
 * </p>
 */
public final class BasicNonByteBufferOperations implements Callable<Boolean> {
  private final TachyonURI mMasterLocation;
  private final TachyonURI mFilePath;
  private final WriteType mWriteType;
  private final ReadType mReadType;
  private final boolean mDeleteIfExists;
  private final int mLength;

  public BasicNonByteBufferOperations(TachyonURI masterLocation, TachyonURI filePath,
      WriteType writeType, ReadType readType, boolean deleteIfExists, int length) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mWriteType = writeType;
    mReadType = readType;
    mDeleteIfExists = deleteIfExists;
    mLength = length;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonFS client = TachyonFS.get(mMasterLocation, new TachyonConf());

    write(client, mFilePath, mWriteType, mDeleteIfExists, mLength);
    return read(client, mFilePath, mReadType);
  }

  private void write(TachyonFS client, TachyonURI filePath, WriteType writeType,
      boolean deleteIfExists, int length) throws IOException {
    // If the file exists already, we will override it.
    TachyonFile file = getOrCreate(client, filePath, deleteIfExists);
    DataOutputStream os = new DataOutputStream(file.getOutStream(writeType));
    try {
      os.writeInt(length);
      for (int i = 0; i < length; i ++) {
        os.writeInt(i);
      }
    } finally {
      os.close();
    }
  }

  private TachyonFile getOrCreate(TachyonFS client, TachyonURI filePath, boolean deleteIfExists)
      throws IOException {
    TachyonFile file = client.getFile(filePath);
    if (file == null) {
      // file doesn't exist yet, so create it
      long fileId = client.createFile(filePath);
      file = client.getFile(fileId);
    } else if (deleteIfExists) {
      // file exists, so delete it and recreate
      client.delete(new TachyonURI(file.getPath()), false);

      long fileId = client.createFile(filePath);
      file = client.getFile(fileId);
    }
    return file;
  }

  private boolean read(TachyonFS client, TachyonURI filePath, ReadType readType)
      throws IOException {
    TachyonFile file = client.getFile(filePath);
    DataInputStream input = new DataInputStream(file.getInStream(readType));
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

    Utils.runExample(new BasicNonByteBufferOperations(new TachyonURI(args[0]), new TachyonURI(
        args[1]), Utils.option(args, 2, WriteType.MUST_CACHE), Utils.option(args, 3,
        ReadType.NO_CACHE), Utils.option(args, 4, true), Utils.option(args, 5, 20)));
  }

  private static void usage() {
    System.out.println("java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
        + BasicNonByteBufferOperations.class.getName()
        + " <master address> <file path> [write type] [read type] [delete file] [num writes]");
    System.exit(-1);
  }
}
