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
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the file's last user specified number of bytes (by default, 1KB)
 * of contents to the console.
 */
@ThreadSafe
public final class TailCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public TailCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "tail";
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws IOException {
    URIStatus status;
    try {
      status = mFileSystem.getStatus(path);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }

    int numOfBytes = -1;
    boolean userSpecifiedBytes = cl.hasOption('C');
    if (userSpecifiedBytes) {
      numOfBytes = Integer.parseInt(cl.getOptionValue('C'));
      Preconditions.checkArgument(numOfBytes > 0, "specified bytes must be > 0");
      userSpecifiedBytes = true;
    }

    if (!status.isFolder()) {
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      FileInStream is = null;
      try {
        is = mFileSystem.openFile(path, options);
        int size = userSpecifiedBytes ? numOfBytes : Constants.KB;
        byte[] buf = new byte[size];
        long bytesToRead = 0L;
        if (status.getLength() > size) {
          bytesToRead = size;
        } else {
          bytesToRead = status.getLength();
        }
        is.skip(status.getLength() - bytesToRead);
        int read = is.read(buf);
        if (read != -1) {
          System.out.write(buf, 0, read);
        }
      } catch (AlluxioException e) {
        throw new IOException(e.getMessage());
      } finally {
        is.close();
      }
    } else {
      throw new IOException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }
  }

  @Override
  public String getUsage() {
    return "tail -C <number of bytes> <path>";
  }

  @Override
  public String getDescription() {
    return "Prints the file's last user specified number of bytes (by default, 1KB) of contents "
        + "to the console.";
  }

  @Override
  protected Options getOptions() {
    Option bytesOption =
        Option.builder("C")
              .required(false)
              .numberOfArgs(1)
              .desc("user specified option")
              .build();
    return new Options().addOption(bytesOption);
  }

  @Override
  public boolean validateArgs(String... args) {
    boolean valid = args.length >= getNumOfArgs();
    if (!valid) {
      System.out.println(getCommandName() + " takes " + getNumOfArgs() + " argument at least\n");
    }
    return valid;
  }
}
