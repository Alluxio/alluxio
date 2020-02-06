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
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Command to run a set of Alluxio commands.
 * Collects information about the Alluxio cluster.
 * */
public class CollectAlluxioInfoCommand extends AbstractInfoCollectorCommand {
  public static final String COMMAND_NAME = "runAlluxioCheck";
  private static final Logger LOG = LoggerFactory.getLogger(CollectAlluxioInfoCommand.class);
  private static final String OUTPUT_FILE_NAME = "alluxioInfo.txt";

  private String mAlluxioPath;
  private List<AlluxioCommand> mAlluxioCommands;

  /**
   * Creates a new instance of {@link CollectAlluxioInfoCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectAlluxioInfoCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
    mAlluxioPath = Paths.get(fsContext.getClusterConf().get(PropertyKey.WORK_DIR), "bin/alluxio")
            .toAbsolutePath().toString();
    registerCommands();
  }

  /**
   * A special shell command that runs an Alluxio cmdline operation.
   * */
  public static class AlluxioCommand extends ShellCommand {
    String mName;
    String mAlluxioPath;
    ShellCommand mAlternative;

    /**
     * Creates an instance of {@link AlluxioCommand}.
     *
     * @param name name of this Alluxio operation
     * @param alluxioPath where Alluxio can be found
     * @param cmd Alluxio cmd to run
     * @param alternative an alternative option if the Alluxio operation fails
     *                    for reasons like the cluster being currently down
     * */
    public AlluxioCommand(String name, String alluxioPath, String cmd, ShellCommand alternative) {
      super((alluxioPath + " " + cmd).split(" "));
      mName = name;
      mAlluxioPath = alluxioPath;
      mAlternative = alternative;
    }

    /**
     * @return whether there is an alternative shell command if the Alluxio command fails
     * */
    public boolean hasAlternativeCommand() {
      return mAlternative != null;
    }

    /**
     * @return the alternative shell command
     * */
    public ShellCommand getAlternativeCommand() {
      return mAlternative;
    }
  }

  private void registerCommands() {
    mAlluxioCommands = new ArrayList<>();
    mAlluxioCommands.add(new AlluxioCommand("getConf", mAlluxioPath,
        "getConf --master --source", null));
    mAlluxioCommands.add(new AlluxioCommand("fsadmin", mAlluxioPath,
        "fsadmin report", null));
    mAlluxioCommands.add(new AlluxioCommand("mount", mAlluxioPath,
        "fs mount", null));
    mAlluxioCommands.add(new AlluxioCommand("version", mAlluxioPath,
        "version -r", null));
    mAlluxioCommands.add(new AlluxioCommand("job", mAlluxioPath,
        "job ls", null));
    mAlluxioCommands.add(new AlluxioCommand("fsadmin", mAlluxioPath,
        "fsadmin report", null));
    mAlluxioCommands.add(new AlluxioCommand("journal", mAlluxioPath,
        String.format("fs ls -R %s",
                mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER)),
                new ShellCommand(new String[]{"ls", "-al", "-R",
                mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER)})));
    // TODO(jiacheng): a command to find lost blocks?
  }

  /**
   * Gets the list of Alluxio operations this command will run.
   *
   * @return a list of {@link AlluxioCommand}
   * */
  public List<AlluxioCommand> getAlluxioCommands() {
    return mAlluxioCommands;
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;

    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);

    StringWriter output = new StringWriter();
    for (AlluxioCommand cmd : getAlluxioCommands()) {
      CommandReturn cr;
      String crStr = "";
      int cmdExitCode = 0;
      boolean cmdCompleted = false;
      try {
        cr = cmd.runWithOutput();
        cmdExitCode = cr.getExitCode();
        if (cr.getExitCode() != 0) {
          crStr = String.format("Command %s failed: %s", cmd, cr.getFormattedOutput());
          LOG.warn(crStr);
        } else {
          // Command completed
          crStr = String.format("Command %s succeeded %s", cmd, cr.getFormattedOutput());
          LOG.info(crStr);
          cmdCompleted = true;
        }
      } catch (IOException e) {
        crStr = String.format("Command %s failed with exception %s", cmd, e.getMessage());
        LOG.warn(crStr);
        if (LOG.isDebugEnabled()) {
          e.printStackTrace();
        }
      }
      output.write(crStr);

      if (!cmdCompleted) {
        if (!cmd.hasAlternativeCommand()) {
          String noAltMsg = String.format("No alternative command for command %s", cmd);
          output.write(noAltMsg);
          continue;
        } else {
          // Try alternative command
          ShellCommand alternativeCmd = cmd.getAlternativeCommand();
          String tryAgainMsg = String.format("Try alternative command %s", alternativeCmd);
          output.write(tryAgainMsg);
          LOG.info(tryAgainMsg);

          String tryAgainRes = "";
          try {
            CommandReturn tryAgain = alternativeCmd.runWithOutput();
            if (tryAgain.getExitCode() != 0) {
              cmdExitCode = tryAgain.getExitCode();
              tryAgainRes = String.format("Alternative command %s failed: %s", alternativeCmd,
                      tryAgain.getFormattedOutput());
              LOG.warn(tryAgainRes);
            } else {
              cmdExitCode = 0;
              tryAgainRes = String.format("Alternative command %s succeeded: %s", alternativeCmd,
                      tryAgain.getFormattedOutput());
              LOG.info(tryAgainRes);
            }
          } catch (IOException e) {
            tryAgainRes = String.format("Alternative command %s failed with exception: %s",
                    alternativeCmd, e.getMessage());
            if (LOG.isDebugEnabled()) {
              e.printStackTrace();
            }
            ret = 1;
          }
          output.write(tryAgainRes);
        }
      }
      // Keep only the larger return code
      if (cmdExitCode > ret) {
        ret = cmdExitCode;
      }
    }

    // TODO(jiacheng): phase 2 consider outputting partial results in a finally block
    File outputFile = generateOutputFile(mWorkingDirPath, OUTPUT_FILE_NAME);
    FileUtils.writeStringToFile(outputFile, output.toString());

    return ret;
  }

  @Override
  public String getUsage() {
    return "runAlluxioCheck";
  }

  @Override
  public String getDescription() {
    return "Run a list of Alluxio commands";
  }
}
