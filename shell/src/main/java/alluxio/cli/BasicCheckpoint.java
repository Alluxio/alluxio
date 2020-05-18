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
import alluxio.RuntimeConstants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

/**
 * An example to show to how use Alluxio's API.
 */
public class BasicCheckpoint implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(BasicCheckpoint.class);

  private final String mFileFolder;
  private final int mNumFiles;
  private final FileSystemContext mFsContext;

  /**
   * @param fileFolder folder to use for the files
   * @param numFiles the number of files
   * @param fsContext filesystem context to use for client operations
   */
  public BasicCheckpoint(String fileFolder, int numFiles, FileSystemContext fsContext) {
    mFileFolder = fileFolder;
    mNumFiles = numFiles;
    mFsContext = fsContext;
  }

  @Override
  public Boolean call() throws Exception {
    FileSystem fs = FileSystem.Factory.create(mFsContext);
    writeFile(fs);
    return readFile(fs);
  }

  private boolean readFile(FileSystem fs) throws IOException, AlluxioException {
    boolean pass = true;
    for (int i = 0; i < mNumFiles; i++) {
      AlluxioURI filePath = new AlluxioURI(mFileFolder + "/part-" + i);
      LOG.debug("Reading data from {}", filePath);
      FileInStream is = fs.openFile(filePath);
      URIStatus status = fs.getStatus(filePath);
      ByteBuffer buf = ByteBuffer.allocate((int) status.getBlockSizeBytes());
      is.read(buf.array());
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < mNumFiles; k++) {
        pass = pass && (buf.getInt() == k);
      }
      is.close();
    }
    return pass;
  }

  private void writeFile(FileSystem fs) throws IOException, AlluxioException {
    for (int i = 0; i < mNumFiles; i++) {
      ByteBuffer buf = ByteBuffer.allocate(80);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < mNumFiles; k++) {
        buf.putInt(k);
      }
      buf.flip();
      AlluxioURI filePath = new AlluxioURI(mFileFolder + "/part-" + i);
      LOG.debug("Writing data to {}", filePath);
      OutputStream os =
          fs.createFile(filePath, CreateFilePOptions.newBuilder().setRecursive(true).build());
      os.write(buf.array());
      os.close();
    }
  }

  /**
   * Example program for using checkpoints.
   * Usage: {@code java -cp <ALLUXIO-VERSION> alluxio.cli.BasicCheckpoint <FileFolder> <Files>}
   *
   * @param args the folder for the files and the files to use
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("java -cp " + RuntimeConstants.ALLUXIO_JAR
          + " alluxio.cli.BasicCheckpoint <FileFolder> <Files>");
      System.exit(-1);
    }
    FileSystemContext fsContext =
        FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
    boolean result = RunTestUtils.runExample(new BasicCheckpoint(args[0], Integer.parseInt(args[1]),
        fsContext));
    System.exit(result ? 0 : 1);
  }
}
