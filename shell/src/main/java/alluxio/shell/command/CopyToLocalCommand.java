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
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.shell.AlluxioShellUtils;

import com.google.common.base.Joiner;
import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.RandomStringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or a directory from the Alluxio filesystem to the local filesystem.
 */
@ThreadSafe
public final class CopyToLocalCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public CopyToLocalCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "copyToLocal";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    System.out
        .println("The \"alluxio fs " + getUsage() + "\" command is deprecated since version 1.5.");
    System.out.println("Use the \"alluxio fs cp <src> <dst>\" command instead. Local Path with "
        + "schema \"file\".");
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    File dstFile = new File(args[1]);
    List<AlluxioURI> srcPaths = AlluxioShellUtils.getAlluxioURIs(mFileSystem, srcPath);
    if (srcPaths.size() == 0) {
      throw new IOException(srcPath.getPath() + " does not exist.");
    }

    if (srcPath.containsWildcard()) {
      copyWildcardToLocal(srcPaths, dstFile);
    } else {
      copyToLocal(srcPath, dstFile);
    }
  }

  /**
   * Copies a list of files or directories specified by srcPaths from the Alluxio filesystem to
   * dstPath in the local filesystem. This method is used when the input path contains wildcards.
   *
   * @param srcPaths the list of files in the Alluxio filesystem
   * @param dstFile the destination directory in the local filesystem
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyWildcardToLocal(List<AlluxioURI> srcPaths, File dstFile)
      throws AlluxioException, IOException {
    if (dstFile.exists() && !dstFile.isDirectory()) {
      throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
    }
    if (!dstFile.exists()) {
      if (!dstFile.mkdirs()) {
        throw new IOException("Fail to create directory: " + dstFile.getPath());
      } else {
        System.out.println("Create directory: " + dstFile.getPath());
      }
    }
    List<String> errorMessages = new ArrayList<>();
    for (AlluxioURI srcPath : srcPaths) {
      try {
        copyToLocal(srcPath, new File(dstFile.getAbsoluteFile(), srcPath.getName()));
      } catch (IOException e) {
        errorMessages.add(e.getMessage());
      }
    }
    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Copies a file or a directory from the Alluxio filesystem to the local filesystem.
   *
   * @param srcPath the source {@link AlluxioURI} (could be a file or a directory)
   * @param dstFile the destination file in the local filesystem
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyToLocal(AlluxioURI srcPath, File dstFile) throws AlluxioException, IOException {
    URIStatus srcStatus = mFileSystem.getStatus(srcPath);

    if (srcStatus.isFolder()) {
      // make a local directory
      if (!dstFile.exists()) {
        if (!dstFile.mkdirs()) {
          throw new IOException("mkdir failure for directory: " + dstFile.getAbsolutePath());
        } else {
          System.out.println("Create directory: " + dstFile.getAbsolutePath());
        }
      }

      List<URIStatus> statuses;
      try {
        statuses = mFileSystem.listStatus(srcPath);
      } catch (AlluxioException e) {
        throw new IOException(e.getMessage());
      }

      List<String> errorMessages = new ArrayList<>();
      for (URIStatus status : statuses) {
        try {
          copyToLocal(
              new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), status.getPath()),
              new File(dstFile.getAbsolutePath(), status.getName()));
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
        }
      }

      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } else {
      copyFileToLocal(srcPath, dstFile);
    }
  }

  /**
   * Copies a file specified by argv from the filesystem to the local filesystem. This is the
   * utility function.
   *
   * @param srcPath The source {@link AlluxioURI} (has to be a file)
   * @param dstFile The destination file in the local filesystem
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyFileToLocal(AlluxioURI srcPath, File dstFile)
      throws AlluxioException, IOException {
    String randomSuffix =
        String.format(".%s_copyToLocal_", RandomStringUtils.randomAlphanumeric(8));
    File tmpDst = new File(dstFile.getAbsolutePath() + randomSuffix);

    try (Closer closer = Closer.create()) {
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      FileInStream is = closer.register(mFileSystem.openFile(srcPath, options));
      FileOutputStream out = closer.register(new FileOutputStream(tmpDst));
      byte[] buf = new byte[64 * Constants.MB];
      int t = is.read(buf);
      while (t != -1) {
        out.write(buf, 0, t);
        t = is.read(buf);
      }
      if (!tmpDst.renameTo(dstFile)) {
        throw new IOException(
            "Failed to rename " + tmpDst.getPath() + " to destination " + dstFile.getPath());
      }
      System.out.println("Copied " + srcPath + " to " + dstFile.getPath());
    } finally {
      tmpDst.delete();
    }
  }

  @Override
  public String getUsage() {
    return "copyToLocal <src> <localDst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory from the Alluxio filesystem to the local filesystem.";
  }
}
