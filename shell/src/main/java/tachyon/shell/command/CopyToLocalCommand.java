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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ReadType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;
import tachyon.shell.TfsShellUtils;

/**
 * Copies a file or a directory from the Tachyon filesystem to the local filesystem.
 */
public final class CopyToLocalCommand extends AbstractTfsShellCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public CopyToLocalCommand(TachyonConf conf, FileSystem tfs) {
    super(conf, tfs);
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
  public void run(String... args) throws IOException {
    TachyonURI srcPath = new TachyonURI(args[0]);
    File dstFile = new File(args[1]);
    List<TachyonURI> srcPaths = TfsShellUtils.getTachyonURIs(mTfs, srcPath);
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
   * Copies a list of files or directories specified by srcPaths from the Tachyon filesystem to
   * dstPath in the local filesystem. This method is used when the input path contains wildcards.
   *
   * @param srcPaths The list of files in the Tachyon filesystem
   * @param dstFile The destination directory in the local filesystem
   * @throws IOException
   */
  private void copyWildcardToLocal(List<TachyonURI> srcPaths,
      File dstFile) throws IOException {
    if (dstFile.exists() && !dstFile.isDirectory()) {
      throw new IOException(
          ExceptionMessage.DESTINATION_FILE_CANNOT_EXIST_WITH_WILDCARD_SOURCE.getMessage());
    }
    if (!dstFile.exists()) {
      if (!dstFile.mkdirs()) {
        throw new IOException("Fail to create directory: " + dstFile.getPath());
      } else {
        System.out.println("Create directory: " + dstFile.getPath());
      }
    }
    List<String> errorMessages = new ArrayList<String>();
    for (TachyonURI srcPath : srcPaths) {
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
   * Copies a file or a directory from the Tachyon filesystem to the local filesystem.
   *
   * @param srcPath The source {@link TachyonURI} (could be a file or a directory)
   * @param dstFile The destination file in the local filesystem
   * @throws IOException
   */
  private void copyToLocal(TachyonURI srcPath, File dstFile) throws IOException {
    TachyonFile srcFd;
    URIStatus srcStatus;
    try {
      srcStatus = mTfs.getStatus(srcPath);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (srcStatus.isFolder()) {
      // make a local directory
      if (!dstFile.exists()) {
        if (!dstFile.mkdirs()) {
          throw new IOException("mkdir failure for directory: " + dstFile.getAbsolutePath());
        } else {
          System.out.println("Create directory: " + dstFile.getAbsolutePath());
        }
      }

      List<URIStatus> statuses = null;
      try {
        statuses = mTfs.listStatus(srcPath);
      } catch (TachyonException e) {
        throw new IOException(e.getMessage());
      }

      List<String> errorMessages = new ArrayList<String>();
      for (URIStatus status : statuses) {
        try {
          copyToLocal(
              new TachyonURI(srcPath.getScheme(), srcPath.getAuthority(), status.getPath()),
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
   * @param srcPath The source {@link TachyonURI} (has to be a file)
   * @param dstFile The destination file in the local filesystem
   * @throws IOException
   */
  private void copyFileToLocal(TachyonURI srcPath, File dstFile) throws IOException {
    try {
      File tmpDst = File.createTempFile("copyToLocal", null);
      tmpDst.deleteOnExit();

      Closer closer = Closer.create();
      try {
        OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
        FileInStream is = closer.register(mTfs.openFile(srcPath, options));
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
        closer.close();
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "copyToLocal <src> <localDst>";
  }
}
