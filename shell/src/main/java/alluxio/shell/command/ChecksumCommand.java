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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Calculates the MD5 checksum for a file.
 */
@ThreadSafe
public final class ChecksumCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public ChecksumCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "checksum";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION);
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI loc = new AlluxioURI(args[0]);
    URIStatus status = mFileSystem.getStatus(loc);
    if (status.isFolder()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(args[0]));
    }
    String str = calcChecksum(loc);
    System.out.println("md5sum: " + str);
  }

  /**
   * Calculate the md5 checksum for a file.
   *
   * @param filePath The {@link AlluxioURI} path of the file calculate MD5 checksum on
   * @return A string representation of the file's MD5 checksum
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private String calcChecksum(AlluxioURI filePath)
      throws AlluxioException, IOException {
    OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
    String csum = null;
    FileInStream fis = mFileSystem.openFile(filePath, options);
    try {
      csum = DigestUtils.md5Hex(fis);
    } catch (Exception e) {
      throw e;
    } finally {
      fis.close();
    }
    return csum;
  }

  @Override
  public String getUsage() {
    return "checksum <loc>";
  }

  @Override
  public String getDescription() {
    return "Calculates the checksum of a file in the Alluxio filesystem.";
  }
}
