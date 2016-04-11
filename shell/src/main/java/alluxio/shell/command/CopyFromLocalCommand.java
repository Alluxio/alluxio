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
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.shell.AlluxioShellUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies the specified file specified by "source path" to the path specified by "remote path".
 * This command will fail if "remote path" already exists.
 */
@ThreadSafe
public final class CopyFromLocalCommand extends AbstractShellCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public CopyFromLocalCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "copyFromLocal";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    String srcPath = args[0];
    AlluxioURI dstPath = new AlluxioURI(args[1]);
    List<File> srcFiles = AlluxioShellUtils.getFiles(srcPath);
    if (srcFiles.size() == 0) {
      throw new IOException("Local path " + srcPath + " does not exist.");
    }
    if (srcPath.contains(AlluxioURI.WILDCARD)) {
      copyFromLocalWildcard(srcFiles, dstPath);
    } else {
      copyFromLocal(new File(srcPath), dstPath);
    }
  }

  /**
   * Copies a directory from local to Alluxio filesystem. The destination directory structure
   * maintained as local directory. This method is used when input path is a directory.
   *
   * @param srcDir The source directory in the local filesystem
   * @param dstPath The {@link AlluxioURI} of the destination
   * @throws IOException if a non-Alluxio related exception occurs
   *
   */
  private void copyFromLocalDir(File srcDir, AlluxioURI dstPath) throws IOException {
    try {
      boolean dstExistedBefore = mFileSystem.exists(dstPath);
      createDstDir(dstPath);
      List<String> errorMessages = Lists.newArrayList();
      File[] fileList = srcDir.listFiles();
      int misFiles = 0;
      for (File srcFile : fileList) {
        AlluxioURI newURI = new AlluxioURI(dstPath, new AlluxioURI(srcFile.getName()));
        try {
          copyPath(srcFile, newURI);
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
          if (!dstExistedBefore && mFileSystem.exists(dstPath)) {
            mFileSystem.delete(dstPath);
          }
        }
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Copies a list of files or directories specified by srcFiles from the local filesystem to
   * dstPath in the Alluxio filesystem space. This method is used when the input path contains
   * wildcards.
   * @param srcFiles A list of source file in the local filesystem
   * @param dstPath The {@link AlluxioURI} of the destination
   * @throws IOException if a non-Alluxio related exception occurs
   */
  private void copyFromLocalWildcard(List<File> srcFiles, AlluxioURI dstPath) throws IOException {
    try {
      boolean dstExistedBefore = mFileSystem.exists(dstPath);
      createDstDir(dstPath);
      List<String> errorMessages = Lists.newArrayList();
      int misFiles = 0;
      for (File srcFile : srcFiles) {
        AlluxioURI newURI = new AlluxioURI(dstPath, new AlluxioURI(srcFile.getName()));
        try {
          copyPath(srcFile, newURI);
          System.out.println("Copied " + srcFile.getPath() + " to " + dstPath);
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
          if (!mFileSystem.exists(newURI)) {
            misFiles++;
          }
        }
      }
      if (errorMessages.size() != 0) {
        if (misFiles == srcFiles.size()) {
          // If the directory doesn't exist and no files were created, then delete the directory
          if (!dstExistedBefore && mFileSystem.exists(dstPath)) {
            mFileSystem.delete(dstPath);
          }
        }
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Create a directory in the Alluxio filesystem space. It will not throw any exception if the
   * destination directory already exists.
   * @param dstPath The {@link AlluxioURI} of the destination directory which will be created
   * @throws IOException if a non-Alluxio related exception occurs
   */
  private void createDstDir(AlluxioURI dstPath) throws IOException {
    try {
      mFileSystem.createDirectory(dstPath);
    } catch (FileAlreadyExistsException e) {
      // it's fine if the directory already exists
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }

    URIStatus dstStatus;
    try {
      dstStatus = mFileSystem.getStatus(dstPath);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
    if (!dstStatus.isFolder()) {
      throw new IOException(
              ExceptionMessage.DESTINATION_FILE_CANNOT_EXIST_WITH_WILDCARD_SOURCE.getMessage());
    }
  }

  /**
   * Copies a file or directory specified by srcPath from the local filesystem to dstPath in the
   * Alluxio filesystem space.
   *
   * @param srcFile The source file in the local filesystem
   * @param dstPath The {@link AlluxioURI} of the destination
   * @throws IOException if a non-Alluxio related exception occurs
   */
  private void copyFromLocal(File srcFile, AlluxioURI dstPath)
      throws IOException {
    if (srcFile.isDirectory()) {
      copyFromLocalDir(srcFile, dstPath);
    } else {
      copyPath(srcFile, dstPath);
    }
    System.out.println("Copied " + srcFile.getPath() + " to " + dstPath);
  }

  /**
   * Copies a file or directory specified by srcPath from the local filesystem to dstPath in the
   * Alluxio filesystem space.
   * @param src The source file in the local filesystem
   * @param dstPath The {@link AlluxioURI} of the destination
   * @throws IOException if a non-Alluxio related exception occurs
   */
  private void copyPath(File src, AlluxioURI dstPath) throws IOException {
    try {
      if (!src.isDirectory()) {
        // If the dstPath is a directory, then it should be updated to be the path of the file where
        // src will be copied to.
        if (mFileSystem.exists(dstPath) && mFileSystem.getStatus(dstPath).isFolder()) {
          dstPath = dstPath.join(src.getName());
        }

        Closer closer = Closer.create();
        FileOutStream os = null;
        try {
          os = closer.register(mFileSystem.createFile(dstPath));
          FileInputStream in = closer.register(new FileInputStream(src));
          FileChannel channel = closer.register(in.getChannel());
          ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
          while (channel.read(buf) != -1) {
            buf.flip();
            os.write(buf.array(), 0, buf.limit());
          }
        } catch (IOException e) {
          // Close the out stream and delete the file, so we don't have an incomplete file lying
          // around.
          if (os != null) {
            os.cancel();
            if (mFileSystem.exists(dstPath)) {
              mFileSystem.delete(dstPath);
            }
          }
          throw e;
        } finally {
          closer.close();
        }
      } else {
        mFileSystem.createDirectory(dstPath);
        List<String> errorMessages = Lists.newArrayList();
        File[] fileList = src.listFiles();
        int misFiles = 0;
        for (File srcFile : fileList) {
          AlluxioURI newURI = new AlluxioURI(dstPath, new AlluxioURI(srcFile.getName()));
          try {
            copyPath(srcFile, newURI);
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
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "copyFromLocal <src> <remoteDst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory from local filesystem to Alluxio filesystem.";
  }
}
