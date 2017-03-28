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
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.shell.AlluxioShellUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Joiner;
import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    if ((dstPath.getScheme() == null || isAlluxio(dstPath.getScheme()))
        && isFile(srcPath.getScheme())) {
      List<File> srcFiles = AlluxioShellUtils.getFiles(srcPath.getPath());
      if (srcFiles.size() == 0) {
        throw new IOException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcPath));
      }
      if (srcPath.containsWildcard()) {
        List<AlluxioURI> srcPaths = new ArrayList<>();
        for (File srcFile : srcFiles) {
          srcPaths.add(
              new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), srcFile.getPath()));
        }
        copyFromLocalWildcard(srcPaths, dstPath);
      } else {
        copyFromLocal(srcPath, dstPath);
      }
    } else if ((srcPath.getScheme() == null || isAlluxio(srcPath.getScheme()))
        && isFile(dstPath.getScheme())) {
      List<AlluxioURI> srcPaths = AlluxioShellUtils.getAlluxioURIs(mFileSystem, srcPath);
      if (srcPaths.size() == 0) {
        throw new IOException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcPath));
      }
      if (srcPath.containsWildcard()) {
        copyWildcardToLocal(srcPaths, dstPath);
      } else {
        copyToLocal(srcPath, dstPath);
      }
    } else if ((srcPath.getScheme() == null || isAlluxio(srcPath.getScheme()))
        && (dstPath.getScheme() == null || isAlluxio(dstPath.getScheme()))) {
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
    } else {
      throw new InvalidPathException(
          "Schemes must be either file or alluxio, and at most one file scheme is allowed.");
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
      System.out.println("Created directory: " + dstPath);
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
   * @param dstPath the {@link AlluxioURI} of the destination path in the Alluxio filesystem
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
          System.out.println("Created directory: " + dstPath);
        }
      }

      if (dstStatus == null) {
        mFileSystem.createDirectory(dstPath);
        System.out.println("Created directory: " + dstPath);
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
      System.out.println("Copied " + srcPath + " to " + dstPath);
    }
  }

  /**
   * Copies a directory from local to Alluxio filesystem. The destination directory structure
   * maintained as local directory. This method is used when input path is a directory.
   *
   * @param srcPath the {@link AlluxioURI} of the source directory in the local filesystem
   * @param dstPath the {@link AlluxioURI} of the destination
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyFromLocalDir(AlluxioURI srcPath, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    File srcDir = new File(srcPath.getPath());
    boolean dstExistedBefore = mFileSystem.exists(dstPath);
    createDstDir(dstPath);
    List<String> errorMessages = new ArrayList<>();
    File[] fileList = srcDir.listFiles();
    if (fileList == null) {
      String errMsg = String.format("Failed to list files for directory %s", srcDir);
      errorMessages.add(errMsg);
      fileList = new File[0];
    }
    int misFiles = 0;
    for (File srcFile : fileList) {
      AlluxioURI newURI = new AlluxioURI(dstPath, new AlluxioURI(srcFile.getName()));
      try {
        copyPath(
            new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), srcFile.getPath()),
            newURI);
      } catch (AlluxioException | IOException e) {
        errorMessages.add(e.getMessage());
        if (!mFileSystem.exists(newURI)) {
          misFiles++;
        }
      }
    }
    if (errorMessages.size() != 0) {
      if (misFiles == fileList.length) {
        // If the directory doesn't exist and no files were created, then delete the directory
        if (!dstExistedBefore && mFileSystem.exists(dstPath)) {
          mFileSystem.delete(dstPath);
        }
      }
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Copies a list of files or directories specified by srcPaths from the local filesystem to
   * dstPath in the Alluxio filesystem space. This method is used when the input path contains
   * wildcards.
   *
   * @param srcPaths a list of files or directories in the local filesystem
   * @param dstPath the {@link AlluxioURI} of the destination
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyFromLocalWildcard(List<AlluxioURI> srcPaths, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    boolean dstExistedBefore = mFileSystem.exists(dstPath);
    createDstDir(dstPath);
    List<String> errorMessages = new ArrayList<>();
    int misFiles = 0;
    for (AlluxioURI srcPath : srcPaths) {
      AlluxioURI newURI = new AlluxioURI(dstPath, new AlluxioURI(srcPath.getName()));
      try {
        copyPath(srcPath, newURI);
        System.out.println("Copied " + srcPath + " to " + dstPath);
      } catch (AlluxioException | IOException e) {
        errorMessages.add(e.getMessage());
        if (!mFileSystem.exists(newURI)) {
          misFiles++;
        }
      }
    }
    if (errorMessages.size() != 0) {
      if (misFiles == srcPaths.size()) {
        // If the directory doesn't exist and no files were created, then delete the directory
        if (!dstExistedBefore && mFileSystem.exists(dstPath)) {
          mFileSystem.delete(dstPath);
        }
      }
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Creates a directory in the Alluxio filesystem space. It will not throw any exception if the
   * destination directory already exists.
   *
   * @param dstPath the {@link AlluxioURI} of the destination directory which will be created
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void createDstDir(AlluxioURI dstPath) throws AlluxioException, IOException {
    try {
      mFileSystem.createDirectory(dstPath);
    } catch (FileAlreadyExistsException e) {
      // it's fine if the directory already exists
    }

    URIStatus dstStatus = mFileSystem.getStatus(dstPath);
    if (!dstStatus.isFolder()) {
      throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
    }
  }

  /**
   * Copies a file or directory specified by srcPath from the local filesystem to dstPath in the
   * Alluxio filesystem space.
   *
   * @param srcPath the {@link AlluxioURI} of the source in the local filesystem
   * @param dstPath the {@link AlluxioURI} of the destination
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyFromLocal(AlluxioURI srcPath, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    File srcFile = new File(srcPath.getPath());
    if (srcFile.isDirectory()) {
      copyFromLocalDir(srcPath, dstPath);
    } else {
      copyPath(srcPath, dstPath);
    }
    System.out.println("Copied " + srcPath + " to " + dstPath);
  }

  /**
   * Copies a file or directory specified by srcPath from the local filesystem to dstPath in the
   * Alluxio filesystem space.
   *
   * @param srcPath the {@link AlluxioURI} of the source file in the local filesystem
   * @param dstPath the {@link AlluxioURI} of the destination
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyPath(AlluxioURI srcPath, AlluxioURI dstPath) throws AlluxioException,
      IOException {
    File src = new File(srcPath.getPath());
    if (!src.isDirectory()) {
      // If the dstPath is a directory, then it should be updated to be the path of the file where
      // src will be copied to.
      if (mFileSystem.exists(dstPath) && mFileSystem.getStatus(dstPath).isFolder()) {
        dstPath = dstPath.join(src.getName());
      }

      FileOutStream os = null;
      try (Closer closer = Closer.create()) {
        os = closer.register(mFileSystem.createFile(dstPath));
        FileInputStream in = closer.register(new FileInputStream(src));
        FileChannel channel = closer.register(in.getChannel());
        ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
        while (channel.read(buf) != -1) {
          buf.flip();
          os.write(buf.array(), 0, buf.limit());
        }
      } catch (Exception e) {
        // Close the out stream and delete the file, so we don't have an incomplete file lying
        // around.
        if (os != null) {
          os.cancel();
          if (mFileSystem.exists(dstPath)) {
            mFileSystem.delete(dstPath);
          }
        }
        throw e;
      }
    } else {
      mFileSystem.createDirectory(dstPath);
      List<String> errorMessages = new ArrayList<>();
      File[] fileList = src.listFiles();
      if (fileList == null) {
        String errMsg = String.format("Failed to list files for directory %s", src);
        errorMessages.add(errMsg);
        fileList = new File[0];
      }
      int misFiles = 0;
      for (File srcFile : fileList) {
        AlluxioURI newURI = new AlluxioURI(dstPath, new AlluxioURI(srcFile.getName()));
        try {
          copyPath(
              new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), srcFile.getPath()),
              newURI);
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
          if (!mFileSystem.exists(newURI)) {
            misFiles++;
          }
        }
      }
      if (errorMessages.size() != 0) {
        if (misFiles == fileList.length) {
          // If the directory doesn't exist and no files were created, then delete the directory
          if (mFileSystem.exists(dstPath)) {
            mFileSystem.delete(dstPath);
          }
        }
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    }
  }

  /**
   * Copies a list of files or directories specified by srcPaths from the Alluxio filesystem to
   * dstPath in the local filesystem. This method is used when the input path contains wildcards.
   *
   * @param srcPaths the list of files in the Alluxio filesystem
   * @param dstPath the {@link AlluxioURI} of the destination directory in the local filesystem
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyWildcardToLocal(List<AlluxioURI> srcPaths, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    File dstFile = new File(dstPath.getPath());
    if (dstFile.exists() && !dstFile.isDirectory()) {
      throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
    }
    if (!dstFile.exists()) {
      if (!dstFile.mkdirs()) {
        throw new IOException("Fail to create directory: " + dstPath);
      } else {
        System.out.println("Create directory: " + dstPath);
      }
    }
    List<String> errorMessages = new ArrayList<>();
    for (AlluxioURI srcPath : srcPaths) {
      try {
        File dstSubFile = new File(dstFile.getAbsoluteFile(), srcPath.getName());
        copyToLocal(srcPath,
            new AlluxioURI(dstPath.getScheme(), dstPath.getAuthority(), dstSubFile.getPath()));
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
   * @param dstPath the {@link AlluxioURI} of the destination in the local filesystem
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyToLocal(AlluxioURI srcPath, AlluxioURI dstPath) throws AlluxioException,
      IOException {
    URIStatus srcStatus = mFileSystem.getStatus(srcPath);
    File dstFile = new File(dstPath.getPath());
    if (srcStatus.isFolder()) {
      // make a local directory
      if (!dstFile.exists()) {
        if (!dstFile.mkdirs()) {
          throw new IOException("mkdir failure for directory: " + dstPath);
        } else {
          System.out.println("Create directory: " + dstPath);
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
          File subDstFile = new File(dstFile.getAbsolutePath(), status.getName());
          copyToLocal(
              new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), status.getPath()),
              new AlluxioURI(dstPath.getScheme(), dstPath.getAuthority(), subDstFile.getPath()));
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
        }
      }

      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } else {
      copyFileToLocal(srcPath, dstPath);
    }
  }

  /**
   * Copies a file specified by argv from the filesystem to the local filesystem. This is the
   * utility function.
   *
   * @param srcPath The source {@link AlluxioURI} (has to be a file)
   * @param dstPath The {@link AlluxioURI} of the destination in the local filesystem
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void copyFileToLocal(AlluxioURI srcPath, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    File dstFile = new File(dstPath.getPath());
    String randomSuffix =
        String.format(".%s_copyToLocal_", RandomStringUtils.randomAlphanumeric(8));
    File outputFile;
    if (dstFile.isDirectory()) {
      outputFile = new File(PathUtils.concatPath(dstFile.getAbsolutePath(), srcPath.getName()));
    } else {
      outputFile = dstFile;
    }
    File tmpDst = new File(outputFile.getPath() + randomSuffix);

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
      if (!tmpDst.renameTo(outputFile)) {
        throw new IOException(
            "Failed to rename " + tmpDst.getPath() + " to destination " + outputFile.getPath());
      }
      System.out.println("Copied " + srcPath + " to " + "file://" + outputFile.getPath());
    } finally {
      tmpDst.delete();
    }
  }

  @Override
  public String getUsage() {
    return "cp [-R] <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory in the Alluxio filesystem or between local filesystem "
        + "and Alluxio filesystem. The -R flag is needed to copy directories in the Alluxio "
        + "filesystem. Local Path with schema \"file\".";
  }

  private static boolean isAlluxio(String scheme) {
    return Constants.SCHEME.equals(scheme);
  }

  private static boolean isFile(String scheme) {
    return "file".equals(scheme);
  }
}
