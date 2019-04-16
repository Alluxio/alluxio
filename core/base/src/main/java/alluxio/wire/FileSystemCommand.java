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

package alluxio.wire;

/**
 * Class to represent a FileSystem command.
 */
public final class FileSystemCommand {
  private CommandType mCommandType;
  private FileSystemCommandOptions mCommandOptions;

  /**
   * Create a new instance of {@link FileSystemCommand}.
   *
   * @param commandType the command type
   * @param commandOptions the options for the specified command
   */
  public FileSystemCommand(CommandType commandType, FileSystemCommandOptions commandOptions) {
    mCommandType = commandType;
    mCommandOptions = commandOptions;
  }

  /**
   * @return the command type
   */
  public CommandType getCommandType() {
    return mCommandType;
  }

  /**
   * @return the command options
   */
  public FileSystemCommandOptions getCommandOptions() {
    return mCommandOptions;
  }

  /**
   * Set the command options.
   *
   * @param commandOptions the command options
   */
  public void setCommandOptions(FileSystemCommandOptions commandOptions) {
    mCommandOptions = commandOptions;
  }

  /**
   * Set the command type.
   *
   * @param commandType the command type
   */
  public void setCommandType(CommandType commandType) {
    mCommandType = commandType;
  }
}
