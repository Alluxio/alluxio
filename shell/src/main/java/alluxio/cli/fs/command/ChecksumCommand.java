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
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Calculates the MD5 checksum for a file.
 */
@ThreadSafe
@PublicApi
public final class ChecksumCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public ChecksumCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(plainPath);
    if (status.isFolder()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_MUST_BE_FILE.getMessage(plainPath.getPath()));
    }

    String str = calculateChecksum(plainPath);
    System.out.println("md5sum: " + str + "\n");
  }

  @Override
  public String getCommandName() {
    return "checksum";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    runWildCardCmd(new AlluxioURI(args[0]), cl);
    return 0;
  }

  /**
   * Calculates the md5 checksum for a file.
   *
   * @param filePath The {@link AlluxioURI} path of the file calculate MD5 checksum on
   * @return A string representation of the file's MD5 checksum
   */
  private String calculateChecksum(AlluxioURI filePath)
      throws AlluxioException, IOException {
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build();
    try (FileInStream fis = mFileSystem.openFile(filePath, options)) {
      return DigestUtils.md5Hex(fis);
    }
  }

  @Override
  public String getUsage() {
    return "checksum <Alluxio path>";
  }

  @Override
  public String getDescription() {
    return "Calculates the md5 checksum of a file in the Alluxio filesystem.";
  }
}
