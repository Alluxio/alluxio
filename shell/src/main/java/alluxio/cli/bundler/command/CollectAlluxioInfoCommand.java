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

package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.shell.ShellCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

/**
 * Command to run a set of Alluxio commands.
 * Collects information about the Alluxio cluster.
 * */
public class CollectAlluxioInfoCommand extends CollectEnvCommand {
  public static final String COMMAND_NAME = "collectAlluxioInfo";
  private static final Logger LOG = LoggerFactory.getLogger(CollectAlluxioInfoCommand.class);

  private String mAlluxioPath;

  /**
   * Creates a new instance of {@link CollectAlluxioInfoCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectAlluxioInfoCommand(FileSystemContext fsContext) {
    super(fsContext);
    mAlluxioPath = Paths.get(fsContext.getClusterConf().get(PropertyKey.WORK_DIR), "bin/alluxio")
            .toAbsolutePath().toString();
    registerCommands();
  }

  /**
   * A special shell command that runs an Alluxio cmdline operation.
   * */
  public static class AlluxioCommand extends ShellCommand {
    String mAlluxioPath;

    /**
     * Creates an instance of {@link AlluxioCommand}.
     *
     * @param alluxioPath where Alluxio can be found
     * @param cmd         Alluxio cmd to run
     */
    public AlluxioCommand(String alluxioPath, String cmd) {
      super(new String[]{alluxioPath, cmd});
      mAlluxioPath = alluxioPath;
    }
  }

  private void registerCommands() {
    // TODO(jiacheng): a command to find lost blocks?
    registerCommand("getConf",
            new AlluxioCommand(mAlluxioPath, "getConf --master --source"), null);
    registerCommand("fsadmin",
            new AlluxioCommand(mAlluxioPath, "fsadmin report"), null);
    registerCommand("mount",
            new AlluxioCommand(mAlluxioPath, "fs mount"), null);
    registerCommand("version",
            new AlluxioCommand(mAlluxioPath, "version -r"), null);
    registerCommand("job",
            new AlluxioCommand(mAlluxioPath, "job ls"), null);
    registerCommand("journal",
            new AlluxioCommand(mAlluxioPath, String.format("fs ls -R %s",
                    mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER))),
            new ShellCommand(new String[]{"ls", "-al", "-R",
                    mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER)}));
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

//  @Override
//  public int run(CommandLine cl) throws AlluxioException, IOException {
//    int ret = 0;
//
//    // Determine the working dir path
//    mWorkingDirPath = getWorkingDirectory(cl);
//
//    StringWriter output = new StringWriter();
//    for (AlluxioCommand cmd : getAlluxioCommands()) {
//      CommandReturn cr;
//      String crStr = "";
//      int cmdExitCode = 0;
//      boolean cmdCompleted = false;
//      try {
//        cr = cmd.runWithOutput();
//        cmdExitCode = cr.getExitCode();
//        if (cr.getExitCode() != 0) {
//          crStr = String.format("Command %s failed: %s", cmd, cr.getFormattedOutput());
//          LOG.warn(crStr);
//        } else {
//          // Command completed
//          crStr = String.format("Command %s succeeded %s", cmd, cr.getFormattedOutput());
//          LOG.info(crStr);
//          cmdCompleted = true;
//        }
//      } catch (IOException e) {
//        crStr = String.format("Command %s failed with exception %s", cmd, e.getMessage());
//        LOG.warn(crStr);
//        LOG.debug("%s", e);
//      }
//      output.write(crStr);
//
//      if (!cmdCompleted) {
//        if (!cmd.hasAlternativeCommand()) {
//          String noAltMsg = String.format("No alternative command for command %s", cmd);
//          output.write(noAltMsg);
//          continue;
//        } else {
//          // Try alternative command
//          ShellCommand alternativeCmd = cmd.getAlternativeCommand();
//          String tryAgainMsg = String.format("Try alternative command %s", alternativeCmd);
//          output.write(tryAgainMsg);
//          LOG.info(tryAgainMsg);
//
//          String tryAgainRes = "";
//          try {
//            CommandReturn tryAgain = alternativeCmd.runWithOutput();
//            if (tryAgain.getExitCode() != 0) {
//              cmdExitCode = tryAgain.getExitCode();
//              tryAgainRes = String.format("Alternative command %s failed: %s", alternativeCmd,
//                      tryAgain.getFormattedOutput());
//              LOG.warn(tryAgainRes);
//            } else {
//              cmdExitCode = 0;
//              tryAgainRes = String.format("Alternative command %s succeeded: %s", alternativeCmd,
//                      tryAgain.getFormattedOutput());
//              LOG.info(tryAgainRes);
//            }
//          } catch (IOException e) {
//            tryAgainRes = String.format("Alternative command %s failed with exception: %s",
//                    alternativeCmd, e.getMessage());
//            LOG.warn(tryAgainRes);
//            LOG.debug("%s", e);
//            ret = 1;
//          }
//          output.write(tryAgainRes);
//        }
//      }
//      // Keep only the larger return code
//      if (cmdExitCode > ret) {
//        ret = cmdExitCode;
//      }
//    }
//
//    // TODO(jiacheng): phase 2 consider outputting partial results in a finally block
//    File outputFile = generateOutputFile(mWorkingDirPath, OUTPUT_FILE_NAME);
//    FileUtils.writeStringToFile(outputFile, output.toString());
//
//    return ret;
//  }

  @Override
  public String getUsage() {
    return "collectAlluxioInfo <outputPath>";
  }

  @Override
  public String getDescription() {
    return "Run a list of Alluxio commands to collect Alluxio cluster information";
  }
}
