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
import alluxio.cli.CommandReader;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Renames a file or directory specified by args. Will fail if the new path name already exists.
 */
@ThreadSafe
public final class MvCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public MvCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);

    mFileSystem.rename(srcPath, dstPath);
    System.out.println("Renamed " + srcPath + " to " + dstPath);
    return 0;
  }
}
