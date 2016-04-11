/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.Version;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

/**
 * Example to show the basic operations of Alluxio.
 */
public class BasicOperations implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final AlluxioURI mMasterLocation;
  private final AlluxioURI mFilePath;
  private final OpenFileOptions mReadOptions;
  private final CreateFileOptions mWriteOptions;
  private final int mNumbers = 20;

  /**
   * @param masterLocation the location of the master
   * @param filePath the path for the files
   * @param readType the {@link ReadType}
   * @param writeType the {@link WriteType}
   */
  public BasicOperations(AlluxioURI masterLocation, AlluxioURI filePath, ReadType readType,
                         WriteType writeType) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mReadOptions = OpenFileOptions.defaults().setReadType(readType);
    mWriteOptions = CreateFileOptions.defaults().setWriteType(writeType);
  }

  @Override
  public Boolean call() throws Exception {
    ClientContext.getConf().set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost());
    ClientContext.getConf().set(Constants.MASTER_RPC_PORT,
        Integer.toString(mMasterLocation.getPort()));
    ClientContext.init();
    FileSystem fs = FileSystem.Factory.get();
    writeFile(fs);
    return readFile(fs);
  }

  private void writeFile(FileSystem fileSystem)
    throws IOException, AlluxioException {
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k++) {
      buf.putInt(k);
    }
    LOG.debug("Writing data...");
    long startTimeMs = CommonUtils.getCurrentMs();
    FileOutStream os = fileSystem.createFile(mFilePath, mWriteOptions);
    os.write(buf.array());
    os.close();

    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "writeFile to file " + mFilePath));
  }

  private boolean readFile(FileSystem fileSystem)
      throws IOException, AlluxioException {
    boolean pass = true;
    LOG.debug("Reading data...");
    final long startTimeMs = CommonUtils.getCurrentMs();
    FileInStream is = fileSystem.openFile(mFilePath, mReadOptions);
    ByteBuffer buf = ByteBuffer.allocate((int) is.remaining());
    is.read(buf.array());
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k++) {
      pass = pass && (buf.getInt() == k);
    }
    is.close();

    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "readFile file " + mFilePath));
    return pass;
  }

  /**
   * Runs the example.
   *
   * Usage:
   * {@code java -cp <ALLUXIO-VERSION> BasicOperations
   * <ReadType (CACHE_PROMOTE | CACHE | NO_CACHE)>
   * <WriteType (MUST_CACHE | CACHE_THROUGH | THROUGH | ASYNC_THROUGH)>}
   *
   * @param args the arguments for this example
   */
  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("java -cp " + Version.ALLUXIO_JAR + " " + BasicOperations.class.getName()
          + " <ReadType (CACHE_PROMOTE | CACHE | NO_CACHE)> <WriteType (MUST_CACHE | CACHE_THROUGH"
          + " | THROUGH | ASYNC_THROUGH)>");
      System.exit(-1);
    }

    Utils.runExample(new BasicOperations(new AlluxioURI(args[0]), new AlluxioURI(args[1]),
        ReadType.valueOf(args[2]), WriteType.valueOf(args[3])));
  }
}
