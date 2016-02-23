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
import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.lineage.AlluxioLineage;
import alluxio.exception.AlluxioException;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Creates a lineage for the given input files, output files, and command line job.
 */
@ThreadSafe
public final class CreateLineageCommand extends AbstractShellCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public CreateLineageCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
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
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    AlluxioLineage tl = AlluxioLineage.get();
    // TODO(yupeng) more validation
    List<AlluxioURI> inputFiles = Lists.newArrayList();
    if (!args[0].equals("noInput")) {
      for (String path : args[0].split(",")) {
        inputFiles.add(new AlluxioURI(path));
      }
    }
    List<AlluxioURI> outputFiles = Lists.newArrayList();
    for (String path : args[1].split(",")) {
      outputFiles.add(new AlluxioURI(path));
    }
    String cmd = "";
    for (int i = 2; i < args.length; i++) {
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
    } catch (AlluxioException e) {
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
