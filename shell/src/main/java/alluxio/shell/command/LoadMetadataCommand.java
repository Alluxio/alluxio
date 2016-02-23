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
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads metadata for the given Alluxio path from UFS.
 */
@ThreadSafe
public final class LoadMetadataCommand extends AbstractShellCommand {

  /**
   * Constructs a new instance to load metadata for the given Alluxio path from UFS.
   *
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public LoadMetadataCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "loadMetadata";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);

    try {
      LoadMetadataOptions options = LoadMetadataOptions.defaults().setRecursive(true);
      mFileSystem.loadMetadata(inputPath, options);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "loadMetadata <path>";
  }

  @Override
  public String getDescription() {
    return "Loads metadata for the given Alluxio path from the under file system.";
  }
}
