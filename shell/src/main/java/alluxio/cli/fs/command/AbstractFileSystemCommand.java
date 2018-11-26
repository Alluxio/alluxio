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
import alluxio.cli.Command;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The base class for all the FileSystem {@link alluxio.cli.Command} classes.
 * It provides a place to hold the {@link FileSystem} client.
 */
@ThreadSafe
public abstract class AbstractFileSystemCommand implements Command {

  protected FileSystem mFileSystem;

  protected AbstractFileSystemCommand(FileSystem fs) {
    mFileSystem = fs;
  }

  /**
   * Runs the command for a particular URI that does not contain wildcard in its path.
   *
   * @param plainPath an AlluxioURI that does not contain wildcard
   * @param cl object containing the original commandLine
   */
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
  }

  /**
   * Processes the header of the command. Our input path may contain wildcard
   * but we only want to print the header for once.
   *
   * @param cl object containing the original commandLine
   */
  protected void processHeader(CommandLine cl) throws IOException {
  }

  /**
   * Runs the command for a particular URI that may contain wildcard in its path.
   *
   * @param wildCardPath an AlluxioURI that may or may not contain a wildcard
   * @param cl object containing the original commandLine
   */
  protected void runWildCardCmd(AlluxioURI wildCardPath, CommandLine cl) throws IOException {
    List<AlluxioURI> paths = FileSystemShellUtils.getAlluxioURIs(mFileSystem, wildCardPath);
    if (paths.size() == 0) { // A unified sanity check on the paths
      throw new IOException(wildCardPath + " does not exist.");
    }
    paths.sort(Comparator.comparing(AlluxioURI::getPath));

    // TODO(lu) if errors occur in runPlainPath, we may not want to print header
    processHeader(cl);

    List<String> errorMessages = new ArrayList<>();
    for (AlluxioURI path : paths) {
      try {
        runPlainPath(path, cl);
      } catch (AlluxioException | IOException e) {
        errorMessages.add(e.getMessage() != null ? e.getMessage() : e.toString());
      }
    }

    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }
}
