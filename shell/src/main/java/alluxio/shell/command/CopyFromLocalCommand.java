/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
import alluxio.util.io.PathUtils;

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
   * Copies a list of files or directories specified by srcFiles from the local filesystem to
   * dstPath in the Alluxio filesystem space. This method is used when the input path contains
   * wildcards.
   *
   * @param srcFiles The list of files in the local filesystem
   * @param dstPath The {@link AlluxioURI} of the destination
   * @throws IOException if a non-Alluxio related exception occurs
   */
  private void copyFromLocalWildcard(List<File> srcFiles, AlluxioURI dstPath) throws IOException {
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

    List<String> errorMessages = Lists.newArrayList();
    for (File srcFile : srcFiles) {
      try {
        copyFromLocal(srcFile,
            new AlluxioURI(PathUtils.concatPath(dstPath.getPath(), srcFile.getName())));
      } catch (IOException e) {
        errorMessages.add(e.getMessage());
      }
    }
    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Copies a file or directory specified by srcPath from the local filesystem to dstPath in the
   * Alluxio filesystem space. Will fail if the path given already exists in the filesystem.
   *
   * @param srcFile The source file in the local filesystem
   * @param dstPath The {@link AlluxioURI} of the destination
   * @throws IOException if a non-Alluxio related exception occurs
   */
  private void copyFromLocal(File srcFile, AlluxioURI dstPath)
      throws IOException {
    copyPath(srcFile, dstPath);
    System.out.println("Copied " + srcFile.getPath() + " to " + dstPath);
  }

  private void copyPath(File src, AlluxioURI dstPath) throws IOException {
    try {
      if (!src.isDirectory()) {
        // If the dstPath is a directory, then it should be updated to be the path of the file where
        // src will be copied to
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
          // around
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
        String[] fileList = src.list();
        for (String file : fileList) {
          AlluxioURI newPath = new AlluxioURI(dstPath, new AlluxioURI(file));
          File srcFile = new File(src, file);
          try {
            copyPath(srcFile, newPath);
          } catch (IOException e) {
            errorMessages.add(e.getMessage());
          }
        }
        if (errorMessages.size() != 0) {
          if (errorMessages.size() == fileList.length) {
            // If no files were created, then delete the directory
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
