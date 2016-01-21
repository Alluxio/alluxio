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

package tachyon.shell;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.reflections.Reflections;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import tachyon.client.file.FileSystem;
import tachyon.conf.TachyonConf;
import tachyon.shell.command.TfsShellCommand;
import tachyon.util.CommonUtils;

/**
 * Class for handling command line inputs.
 */
public class TfsShell implements Closeable {
  /**
   * Main method, starts a new TfsShell.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @throws IOException if closing the shell fails
   */
  public static void main(String[] argv) throws IOException {
    TfsShell shell = new TfsShell(new TachyonConf());
    int ret;
    try {
      ret = shell.run(argv);
    } finally {
      shell.close();
    }
    System.exit(ret);
  }

  private final Map<String, TfsShellCommand> mCommands = Maps.newHashMap();
  private final TachyonConf mTachyonConf;
  private final FileSystem mTfs;

  /**
   * @param tachyonConf the configuration for Tachyon
   */
  public TfsShell(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mTfs = FileSystem.Factory.get();
    loadCommands();
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Uses reflection to get all the {@link TfsShellCommand} classes and store them in a map.
   */
  private void loadCommands() {
    String pkgName = TfsShellCommand.class.getPackage().getName();
    Reflections reflections = new Reflections(pkgName);
    for (Class<? extends TfsShellCommand> cls : reflections.getSubTypesOf(TfsShellCommand.class)) {
      // Only instantiate a concrete class
      if (!Modifier.isAbstract(cls.getModifiers())) {
        TfsShellCommand cmd;
        try {
          cmd = CommonUtils.createNewClassInstance(cls,
              new Class[] { TachyonConf.class, FileSystem.class },
              new Object[] { mTachyonConf, mTfs });
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        mCommands.put(cmd.getCommandName(), cmd);
      }
    }
  }

  /**
   * Method which prints the method to use all the commands.
   */
  private void printUsage() {
    System.out.println("Usage: java TfsShell");
    SortedSet<String> sortedCmds = new TreeSet<String>(mCommands.keySet());
    for (String cmd : sortedCmds) {
      System.out.format("%-60s%-95s%n", "       [" + mCommands.get(cmd).getUsage() + "]   ",
                      mCommands.get(cmd).getDescription());
    }
  }

  /**
   * Method which determines how to handle the user's request, will display usage help to the user
   * if command format is incorrect.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred
   */
  public int run(String... argv) {
    if (argv.length == 0) {
      printUsage();
      return -1;
    }

    // Sanity check on the number of arguments
    String cmd = argv[0];
    TfsShellCommand command = mCommands.get(cmd);

    if (command == null) { // Unknown command (we didn't find the cmd in our dict)
      System.out.println(cmd + " is an unknown command.\n");
      printUsage();
      return -1;
    }

    String[] args = Arrays.copyOfRange(argv, 1, argv.length);
    if (!command.validateArgs(args)) {
      printUsage();
      return -1;
    }

    // Handle the command
    try {
      command.run(args);
      return 0;
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
      return -1;
    }
  }
}
