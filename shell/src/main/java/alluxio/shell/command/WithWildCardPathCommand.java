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
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.shell.AlluxioShellUtils;

import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An abstract class for the commands that take exactly one path that could contain wildcard
 * characters.
 *
 * It will first do a glob against the input pattern then run the command for each expanded path.
 */
@ThreadSafe
public abstract class WithWildCardPathCommand extends AbstractShellCommand {

  protected WithWildCardPathCommand(FileSystem fs) {
    super(fs);
  }

  /**
   * Actually runs the command against one expanded path.
   *
   * @param path the expanded input path
   * @param cl the parsed command line object including options
   */
  protected abstract void runCommand(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException;

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    for (String arg : args) {
      AlluxioURI inputPath = new AlluxioURI(arg);

      List<AlluxioURI> paths = AlluxioShellUtils.getAlluxioURIs(mFileSystem, inputPath);
      if (paths.size() == 0) { // A unified sanity check on the paths
        throw new IOException(inputPath + " does not exist.");
      }
      Collections.sort(paths, createAlluxioURIComparator());

      List<String> errorMessages = new ArrayList<>();
      for (AlluxioURI path : paths) {
        try {
          runCommand(path, cl);
        } catch (AlluxioException | IOException e) {
          errorMessages.add(e.getMessage());
        }
      }

      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    }
    return 0;
  }

  private static Comparator<AlluxioURI> createAlluxioURIComparator() {
    return new Comparator<AlluxioURI>() {
      @Override
      public int compare(AlluxioURI tUri1, AlluxioURI tUri2) {
        // ascending order
        return tUri1.getPath().compareTo(tUri2.getPath());
      }
    };
  }
}
