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
import java.util.List;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.file.FileSystem;
import tachyon.client.lineage.TachyonLineage;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.job.CommandLineJob;
import tachyon.job.JobConf;

/**
 * Creates a lineage for the given input files, output files, and command line job.
 */
public final class CreateLineageCommand extends AbstractTfsShellCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public CreateLineageCommand(TachyonConf conf, FileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "createLineage";
  }

  @Override
  protected int getNumOfArgs() {
    return 3;
  }

  @Override
  public boolean validateArgs(String... args) {
    boolean valid = args.length >= getNumOfArgs();
    if (!valid) {
      System.out.println(getCommandName() + " takes at least" + getNumOfArgs() + " arguments, "
              + " not " + args.length + "\n");
    }
    return valid;
  }

  @Override
  public void run(String... args) throws IOException {
    TachyonLineage tl = TachyonLineage.get();
    // TODO(yupeng) more validation
    List<TachyonURI> inputFiles = Lists.newArrayList();
    if (!args[0].equals("noInput")) {
      for (String path : args[0].split(",")) {
        inputFiles.add(new TachyonURI(path));
      }
    }
    List<TachyonURI> outputFiles = Lists.newArrayList();
    for (String path : args[1].split(",")) {
      outputFiles.add(new TachyonURI(path));
    }
    String cmd = "";
    for (int i = 2; i < args.length; i ++) {
      cmd += args[i] + " ";
    }

    String outputPath = ClientContext.getConf().get(Constants.MASTER_LINEAGE_RECOMPUTE_LOG_PATH);
    if (outputPath == null) {
      throw new IOException("recompute output log is not configured");
    }
    CommandLineJob job = new CommandLineJob(cmd, new JobConf(outputPath));
    long lineageId;
    try {
      lineageId = tl.createLineage(inputFiles, outputFiles, job);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    System.out.println("Lineage " + lineageId + " has been created.");
  }

  @Override
  public String getUsage() {
    return "createLineage <inputFile1,...> <outputFile1,...> [<cmd_arg1> <cmd_arg2> ...]";
  }

  @Override
  public String getDescription() {
    return "Creates a lineage.";
  }
}
