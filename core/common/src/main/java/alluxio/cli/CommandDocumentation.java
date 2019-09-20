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

package alluxio.cli;

import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;

/**
 * command object for reading yaml files.
 */
public class CommandDocumentation {
  private String mName;
  private  String mUsage;
  private String mDescription;
  private String mExamples;
  private  String mSubCommands;
  private String[] mOptions;

  /**
   * Set the name of the command.
   *
   * @param name of command
   */
  public void setName(String name) {
    mName = name;
  }

  /**
   * get the name of the command.
   *
   * @return command name
   */
  public String getName() {
    return mName;
  }

  /**
   * Set the usage of the command.
   *
   * @param Usage of command
   */
  public void setUsage(String Usage) {
    mUsage = Usage;
  }

  /**
   * Get the usage of the command.
   *
   * @return command usage
   */
  public String getUsage() {
    return mUsage;
  }

  /**
   * Set the description of the command.
   *
   * @param Description of command
   */
  public void setDescription(String Description) {
    mDescription = Description;
  }

  /**
   * Get the description of the command.
   *
   * @return command description
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * Set the examples of the command.
   *
   * @param Examples of command
   */
  public void setExamples(String Examples) {
    mExamples = Examples;
  }

  /**
   * Get the examples of the command.
   *
   * @return example command
   */
  public  String getExamples() {
    return mExamples;
  }

  /**
   * Set the subCommands of the command.
   *
   * @param subCommands of command
   */
  @Nullable
  public void setSubCommands(String subCommands) {
    mSubCommands = subCommands;
  }

  /**
   * Get the subCommands of the command.
   *
   * @return command subcommands
   */
  public String getSubCommands() {
    return mSubCommands;
  }

  /**
   * Set the options of the command.
   *
   * @param options of command
   */
  @Nullable
  public void setOptions(String[] options) {
    if (options != null) {
      mOptions = options.clone();
    }
  }

  /**
   * Get the options of the command.
  *
  * @return command options
  */
  public String[] getOptions() {
    return mOptions;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("Name:", getName())
        .add("Usage:", getUsage())
        .add("Description:", getDescription()).toString();
  }
}
