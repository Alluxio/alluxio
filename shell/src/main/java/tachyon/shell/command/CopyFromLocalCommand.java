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

package tachyon.shell.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.TachyonException;
import tachyon.shell.TfsShellUtils;
import tachyon.thrift.FileInfo;
import tachyon.util.io.PathUtils;

/**
 * Copies the specified file specified by "source path" to the path specified by "remote path".
 * This command will fail if "remote path" already exists.
 */
public final class CopyFromLocalCommand extends AbstractTfsShellCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public CopyFromLocalCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
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
  public void run(String... args) throws IOException {
    String srcPath = args[0];
    TachyonURI dstPath = new TachyonURI(args[1]);
    List<File> srcFiles = TfsShellUtils.getFiles(srcPath);
    if (srcFiles.size() == 0) {
      throw new IOException("Local path " + srcPath + " does not exist.");
    }

    if (srcPath.contains(TachyonURI.WILDCARD)) {
      copyFromLocalWildcard(srcFiles, dstPath);
    } else {
      copyFromLocal(new File(srcPath), dstPath);
    }
  }

  /**
   * Copies a list of files or directories specified by srcFiles from the local filesystem to
   * dstPath in the Tachyon filesystem space. This method is used when the input path contains
   * wildcards.
   *
   * @param srcFiles The list of files in the local filesystem
   * @param dstPath The {@link TachyonURI} of the destination
   * @throws IOException if a non-Tachyon related exception occurs
   */
  private void copyFromLocalWildcard(List<File> srcFiles, TachyonURI dstPath) throws IOException {
    try {
      mTfs.mkdir(dstPath);
    } catch (FileAlreadyExistsException e) {
      // it's fine if the directory already exists
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    FileInfo dstFileInfo;
    try {
      TachyonFile dstFd = mTfs.open(dstPath);
      dstFileInfo = mTfs.getInfo(dstFd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    if (!dstFileInfo.isFolder) {
      throw new IOException(
          ExceptionMessage.DESTINATION_FILE_CANNOT_EXIST_WITH_WILDCARD_SOURCE.getMessage());
    }

    List<String> errorMessages = Lists.newArrayList();
    for (File srcFile : srcFiles) {
      try {
        copyFromLocal(srcFile,
            new TachyonURI(PathUtils.concatPath(dstPath.getPath(), srcFile.getName())));
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
   * Tachyon filesystem space. Will fail if the path given already exists in the filesystem.
   *
   * @param srcFile The source file in the local filesystem
   * @param dstPath The {@link TachyonURI} of the destination
   * @throws IOException if a non-Tachyon related exception occurs
   */
  private void copyFromLocal(File srcFile, TachyonURI dstPath)
      throws IOException {
    copyPath(srcFile, dstPath);
    System.out.println("Copied " + srcFile.getPath() + " to " + dstPath);
  }

  private void copyPath(File src, TachyonURI dstPath) throws IOException {
    try {
      if (!src.isDirectory()) {
        // If the dstPath is a directory, then it should be updated to be the path of the file where
        // src will be copied to
        TachyonFile fd = mTfs.openIfExists(dstPath);
        if (fd != null) {
          FileInfo tFile = mTfs.getInfo(fd);
          if (tFile.isFolder) {
            dstPath = dstPath.join(src.getName());
          }
        }

        Closer closer = Closer.create();
        FileOutStream os = null;
        try {
          os = closer.register(mTfs.getOutStream(dstPath, OutStreamOptions.defaults()));
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
            fd = mTfs.openIfExists(dstPath);
            if (fd != null) {
              mTfs.delete(fd);
            }
          }
          throw e;
        } finally {
          closer.close();
        }
      } else {
        mTfs.mkdir(dstPath);
        List<String> errorMessages = Lists.newArrayList();
        String[] fileList = src.list();
        for (String file : fileList) {
          TachyonURI newPath = new TachyonURI(dstPath, new TachyonURI(file));
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
            TachyonFile f = mTfs.openIfExists(dstPath);
            if (f != null) {
              mTfs.delete(f);
            }
          }
          throw new IOException(Joiner.on('\n').join(errorMessages));
        }
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "copyFromLocal <src> <remoteDst>";
  }

  @Override
  public String getDescription() {
    return "Copies the specified file specified by \"source path\" to the path specified by \"remote path\".";
  }
}
