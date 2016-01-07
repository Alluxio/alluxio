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

import java.io.IOException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ReadType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;

/**
 * Prints the file's last 1KB of contents to the console.
 */
public final class TailCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public TailCommand(TachyonConf conf, FileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "tail";
  }

  @Override
  void runCommand(TachyonURI path) throws IOException {
    URIStatus status;
    try {
      status = mTfs.getStatus(path);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (!status.isFolder()) {
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      FileInStream is = null;
      try {
        is = mTfs.openFile(path, options);
        byte[] buf = new byte[Constants.KB];
        long bytesToRead = 0L;
        if (status.getLength() > Constants.KB) {
          bytesToRead = Constants.KB;
        } else {
          bytesToRead = status.getLength();
        }
        is.skip(status.getLength() - bytesToRead);
        int read = is.read(buf);
        if (read != -1) {
          System.out.write(buf, 0, read);
        }
      } catch (TachyonException e) {
        throw new IOException(e.getMessage());
      } finally {
        is.close();
      }
    } else {
      throw new IOException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }
  }

  @Override
  public String getUsage() {
    return "tail <path>";
  }
}
