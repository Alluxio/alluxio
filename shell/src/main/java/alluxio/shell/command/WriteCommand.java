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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Usage: write dst s n.
 * Write n files to dst_{i} (0 <= i < n). Each file has size of s.
 */
@ThreadSafe
public final class WriteCommand extends AbstractShellCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public WriteCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "write";
  }

  @Override
  protected int getNumOfArgs() {
    return 3;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    String filePrefix = args[0];
    try {
      int size = Integer.parseInt(args[1]);
      int n = Integer.parseInt(args[2]);
      byte[] content = null;
      if (size > 0) {
        content = new byte[size];
        Arrays.fill(content, (byte) 'a');
      }
      long start = System.nanoTime();
      write(filePrefix, content, n);
      long end = System.nanoTime();
      System.out.printf("%f\n", (end - start)/1000000.0/1000.0);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Invalid args " + args[1] + ", " + args[2] + ".", e);
    }
  }

  /**
   * Write files.
   * @param filePrefix The filePrefix to write to.
   * @param content The content.
   * @param n The number of files.
   * @throws IOException if a non-Alluxio related exception occurs.
   */
  private void write(String filePrefix, byte[] content, int n) throws IOException {
    try {
      for (int i = 0; i < n; i++) {
        AlluxioURI dstPath = new AlluxioURI(filePrefix + "_" + Integer.toString(i));
        if (mFileSystem.exists(dstPath)) {
          mFileSystem.delete(dstPath);
        }

        FileOutStream os = null;
        Closer closer = Closer.create();
        try {
          os = closer.register(mFileSystem.createFile(dstPath));
          if (content != null) os.write(content, 0, content.length);
        } catch (IOException e) {
          if (os != null) {
            os.cancel();
            if (mFileSystem.exists(dstPath)) {
              mFileSystem.delete(dstPath);
            }
            throw e;
          }
        } finally {
          closer.close();
        }
      }
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public String getUsage() {
    return "write prefix size n";
  }

  @Override
  public String getDescription() {
    return "Write files with certain size Alluxio filesystem.";
  }
}
