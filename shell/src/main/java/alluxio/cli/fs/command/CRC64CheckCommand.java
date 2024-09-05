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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * The CLI for CRC64 check.
 */
public class CRC64CheckCommand extends AbstractFileSystemCommand {
  /**
   * The constructor.
   * @param fsContext the fs context
   */
  public CRC64CheckCommand(
      @Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    System.out.println("Checking " + plainPath);
    long crc64 = CRC64CheckCommandUtils.checkCRC64(mFsContext, mFileSystem, plainPath);
    System.out.println("CRC64 check for file " + plainPath + " succeeded. "
        + "CRC64: " + Long.toHexString(crc64));
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    for (String dirArg : args) {
      AlluxioURI path = new AlluxioURI(dirArg);
      runPlainPath(path, cl);
    }
    return 0;
  }

  @Override
  public String getCommandName() {
    return "crc64check";
  }

  @Override
  public String getUsage() {
    return "crc64check <path> ...";
  }

  @Override
  public String getDescription() {
    return "Does the CRC check on a given alluxio path. The UFS must support CRC64 checksum and "
        + "the file must be fully cached on alluxio.";
  }
}
