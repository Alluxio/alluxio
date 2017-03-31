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

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;
import org.apache.commons.cli.CommandLine;
import org.reflections.Reflections;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Command for print help message for the given command.
 * If there isn't given command, print help messages for all supported commands.
 */
@ThreadSafe
public final class HelpCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public HelpCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "help";
  }

  @Override
  public boolean validateArgs(String... args) {
    boolean valid = args.length >= getNumOfArgs();
    if (!valid) {
      System.out.println(getCommandName() + " takes at least " + getNumOfArgs() + " arguments, "
              + " not " + args.length + "\n");
    }
    return valid;
  }

  private final Map<String, ShellCommand> mCommands = new HashMap<>();

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    loadCommands();
    SortedSet<String> sortedCmds = null;
    boolean hasUnknownCommand = false;
    if (args.length == 0) {
      //print help messages for all supported commands.
      sortedCmds = new TreeSet<>(mCommands.keySet());
    } else {
      sortedCmds = new TreeSet<>();
      for (String cmd : args) {
        if (mCommands.containsKey(cmd)) {
          sortedCmds.add(cmd);
        } else {
          hasUnknownCommand = true;
          System.out.println(cmd + " is an unknown command.");
        }
      }
    }
    if (hasUnknownCommand) {
      System.out.println();
    }
    printUsage(sortedCmds);
  }

  private void loadCommands() {
    String pkgName = ShellCommand.class.getPackage().getName();
    Reflections reflections = new Reflections(pkgName);
    for (Class<? extends ShellCommand> cls : reflections.getSubTypesOf(ShellCommand.class)) {
      // Only instantiate a concrete class
      if (!Modifier.isAbstract(cls.getModifiers())) {
        ShellCommand cmd;
        try {
          cmd = CommonUtils.createNewClassInstance(cls,
              new Class[] { FileSystem.class },
              new Object[] {mFileSystem });
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        mCommands.put(cmd.getCommandName(), cmd);
      }
    }
  }

  private void printUsage(SortedSet<String> sortedCmds) {
    if (sortedCmds == null || sortedCmds.size() == 0) {
      return;
    }
    for (String cmd : sortedCmds) {
      System.out.format("%-60s%-95s%n", "       [" + mCommands.get(cmd).getUsage() + "]   ",
          mCommands.get(cmd).getDescription());
    }
  }

  @Override
  public String getUsage() {
    return "help <command>";
  }

  @Override
  public String getDescription() {
    return "Prints help message for the given command. "
        + "If there isn't given command, prints help messages for all supported commands.";
  }

  @Override
  protected int getNumOfArgs() {
    return 0;
  }

}
