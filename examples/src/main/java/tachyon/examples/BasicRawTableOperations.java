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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.ReadType;
import tachyon.client.WriteType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.client.table.RawColumn;
import tachyon.client.table.RawTable;
import tachyon.client.table.TachyonRawTables;
import tachyon.exception.TachyonException;

public class BasicRawTableOperations implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int COLS = 3;

  private final TachyonURI mMasterAddress;
  private final TachyonURI mTablePath;
  private final ReadType mReadType;
  private final WriteType mWriteType;
  private final int mDataLength = 20;
  private final int mMetadataLength = 5;

  public BasicRawTableOperations(TachyonURI masterAddress, TachyonURI tablePath,
      ReadType readType, WriteType writeType) {
    mMasterAddress = masterAddress;
    mTablePath = tablePath;
    mReadType = readType;
    mWriteType = writeType;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonRawTables tachyonRawTableClient = TachyonRawTables.TachyonRawTablesFactory.get();
    FileSystem tachyonClient = FileSystem.Factory.get();
    createRawTable(tachyonRawTableClient);
    write(tachyonRawTableClient);
    return read(tachyonRawTableClient, tachyonClient);
  }

  private void createRawTable(TachyonRawTables tachyonRawTableClient)
      throws IOException, TachyonException {
    ByteBuffer data = ByteBuffer.allocate(mMetadataLength * 4);
    data.order(ByteOrder.nativeOrder());
    for (int k = -mMetadataLength; k < 0; k ++) {
      data.putInt(k);
    }
    data.flip();
    tachyonRawTableClient.create(mTablePath, 3, data);
  }

  private boolean read(TachyonRawTables tachyonRawTableClient, FileSystem tachyonClient)
      throws IOException, TachyonException {
    boolean pass = true;

    LOG.debug("Reading data...");
    RawTable rawTable = tachyonRawTableClient.open(mTablePath);
    ByteBuffer metadata = tachyonRawTableClient.getInfo(rawTable).metadata;
    LOG.debug("Metadata: " + metadata);
    metadata.order(ByteOrder.nativeOrder());
    for (int k = -mMetadataLength; k < 0; k ++) {
      pass = pass && (metadata.getInt() == k);
    }

    for (int column = 0; column < COLS; column ++) {
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(mReadType);
      RawColumn rawColumn = rawTable.getColumn(column);
      FileInStream is =
          tachyonClient.openFile(tachyonRawTableClient.getPartitionUri(rawColumn, 0), options);
      ByteBuffer buf = ByteBuffer.allocate(mDataLength * 4);
      is.read(buf.array());
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < mDataLength; k ++) {
        pass = pass && (buf.getInt() == k);
      }
      is.close();
    }
    return pass;
  }

  private void write(TachyonRawTables tachyonRawTableClient) throws IOException, TachyonException {
    RawTable rawTable = tachyonRawTableClient.open(mTablePath);

    LOG.debug("Writing data...");
    for (int column = 0; column < COLS; column ++) {
      ByteBuffer buf = ByteBuffer.allocate(mDataLength * 4);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < mDataLength; k ++) {
        buf.putInt(k);
      }
      buf.flip();

      RawColumn rawColumn = rawTable.getColumn(column);
      FileOutStream os = tachyonRawTableClient.createPartition(rawColumn, 0);
      os.write(buf.array());
      os.close();
    }
  }

  public static void main(String[] args) throws IllegalArgumentException {
    if (args.length != 4) {
      System.out.println("java -cp " + Version.TACHYON_JAR + " "
          + BasicRawTableOperations.class.getName() + " <master address> <file path> "
          + " <ReadType (CACHE_PROMOTE | CACHE | NO_CACHE)> <WriteType (MUST_CACHE | CACHE_THROUGH"
          + " | THROUGH)>");
      System.exit(-1);
    }
    Utils.runExample(new BasicRawTableOperations(new TachyonURI(args[0]), new TachyonURI(args[1]),
        ReadType.valueOf(args[2]), WriteType.valueOf(args[3])));
  }
}
