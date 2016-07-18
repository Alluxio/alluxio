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
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.shell.AlluxioShellUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Joiner;
import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or a directory in the Alluxio filesystem.
 */
@ThreadSafe
public final class CpCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public CpCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "cp";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION);
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);
    List<AlluxioURI> srcPaths = AlluxioShellUtils.getAlluxioURIs(mFileSystem, srcPath);
    if (srcPaths.size() == 0) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcPath.getPath()));
    }

    if (srcPath.containsWildcard()) {
      copyWildcard(srcPaths, dstPath, cl.hasOption("R"));
    } else {
      copy(srcPath, dstPath, cl.hasOption("R"));
    }
  }

  /**
   * Copies a list of files or directories specified by srcPaths to the destination specified by
   * dstPath. This method is used when the original source path contains wildcards.
   *
   * @param srcPaths a list of files or directories in the Alluxio filesystem
   * @param dstPath the destination in the Alluxio filesystem
   * @param recursive indicates whether directories should be copied recursively
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyWildcard(List<AlluxioURI> srcPaths, AlluxioURI dstPath, boolean recursive)
      throws AlluxioException, IOException {
    URIStatus dstStatus = null;
    try {
      dstStatus = mFileSystem.getStatus(dstPath);
    } catch (FileDoesNotExistException e) {
      // if the destination does not exist, it will be created
    }

    if (dstStatus != null && !dstStatus.isFolder()) {
      throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
    }
    if (dstStatus == null) {
      mFileSystem.createDirectory(dstPath);
      System.out.println("Created directory: " + dstPath.getPath());
    }
    List<String> errorMessages = new ArrayList<>();
    for (AlluxioURI srcPath : srcPaths) {
      try {
        copy(srcPath, new AlluxioURI(dstPath.getScheme(), dstPath.getAuthority(),
            PathUtils.concatPath(dstPath.getPath(), srcPath.getName())), recursive);
      } catch (AlluxioException | IOException e) {
        errorMessages.add(e.getMessage());
      }
    }
    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Copies a file or a directory in the Alluxio filesystem.
   *
   * @param srcPath the source {@link AlluxioURI} (could be a file or a directory)
   * @param dstPath the destination path in the Alluxio filesystem
   * @param recursive indicates whether directories should be copied recursively
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copy(AlluxioURI srcPath, AlluxioURI dstPath, boolean recursive)
      throws AlluxioException, IOException {
    URIStatus srcStatus = mFileSystem.getStatus(srcPath);

    URIStatus dstStatus = null;
    try {
      dstStatus = mFileSystem.getStatus(dstPath);
    } catch (FileDoesNotExistException e) {
      // if the destination does not exist, it will be created
    }

    if (!srcStatus.isFolder()) {
      if (dstStatus != null && dstStatus.isFolder()) {
        dstPath = new AlluxioURI(PathUtils.concatPath(dstPath.getPath(), srcPath.getName()));
      }
      copyFile(srcPath, dstPath);
    } else {
      if (!recursive) {
        throw new IOException(
            srcPath.getPath() + " is a directory, to copy it please use \"cp -R <src> <dst>\"");
      }

      List<URIStatus> statuses;
      statuses = mFileSystem.listStatus(srcPath);

      if (dstStatus != null) {
        if (!dstStatus.isFolder()) {
          throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
        }
        // if copying a directory to an existing directory, the copied directory will become a
        // subdirectory of the destination
        if (srcStatus.isFolder()) {
          dstPath = new AlluxioURI(PathUtils.concatPath(dstPath.getPath(), srcPath.getName()));
          mFileSystem.createDirectory(dstPath);
          System.out.println("Created directory: " + dstPath.getPath());
        }
      }

      if (dstStatus == null) {
        mFileSystem.createDirectory(dstPath);
        System.out.println("Created directory: " + dstPath.getPath());
      }

      List<String> errorMessages = new ArrayList<>();
      for (URIStatus status : statuses) {
        try {
          copy(new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), status.getPath()),
              new AlluxioURI(dstPath.getScheme(), dstPath.getAuthority(),
                  PathUtils.concatPath(dstPath.getPath(), status.getName())), recursive);
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
        }
      }

      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    }
  }

  /**
   * Copies a file in the Alluxio filesystem.
   *
   * @param srcPath the source {@link AlluxioURI} (has to be a file)
   * @param dstPath the destination path in the Alluxio filesystem
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyFile(AlluxioURI srcPath, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    try (Closer closer = Closer.create()) {
      OpenFileOptions openFileOptions = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      FileInStream is = closer.register(mFileSystem.openFile(srcPath, openFileOptions));
      CreateFileOptions createFileOptions = CreateFileOptions.defaults();
      FileOutStream os = closer.register(mFileSystem.createFile(dstPath, createFileOptions));
      IOUtils.copy(is, os);
      System.out.println("Copied " + srcPath + " to " + dstPath.getPath());
    }
  }

  @Override
  public String getUsage() {
    return "cp [-R] <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory in the Alluxio filesystem. "
        + "The -R flag is needed to copy directories.";
  }
}
