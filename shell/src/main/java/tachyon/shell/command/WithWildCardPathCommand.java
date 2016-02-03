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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Joiner;

import org.apache.commons.cli.CommandLine;

import tachyon.TachyonURI;
import tachyon.client.file.FileSystem;
import tachyon.conf.TachyonConf;
import tachyon.shell.TfsShellUtils;

/**
 * An abstract class for the commands that take exactly one path that could contain wildcard
 * characters.
 *
 * It will first do a glob against the input pattern then run the command for each expanded path.
 */
@ThreadSafe
public abstract class WithWildCardPathCommand extends AbstractTfsShellCommand {

  protected WithWildCardPathCommand(TachyonConf conf, FileSystem fs) {
    super(conf, fs);
  }

  /**
   * Actually runs the command against one expanded path.
   *
   * @param path the expanded input path
   * @throws IOException if the command fails
   */
  abstract void runCommand(TachyonURI path) throws IOException;

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    TachyonURI inputPath = new TachyonURI(args[0]);

    List<TachyonURI> paths = TfsShellUtils.getTachyonURIs(mFileSystem, inputPath);
    if (paths.size() == 0) { // A unified sanity check on the paths
      throw new IOException(inputPath + " does not exist.");
    }
    Collections.sort(paths, createTachyonURIComparator());

    List<String> errorMessages = new ArrayList<String>();
    for (TachyonURI path : paths) {
      try {
        runCommand(path);
      } catch (IOException e) {
        errorMessages.add(e.getMessage());
      }
    }

    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  private static Comparator<TachyonURI> createTachyonURIComparator() {
    return new Comparator<TachyonURI>() {
      @Override
      public int compare(TachyonURI tUri1, TachyonURI tUri2) {
        // ascending order
        return tUri1.getPath().compareTo(tUri2.getPath());
      }
    };
  }
}
