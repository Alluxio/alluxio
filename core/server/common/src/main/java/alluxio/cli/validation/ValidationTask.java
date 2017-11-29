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

package alluxio.cli.validation;

import alluxio.exception.status.InvalidArgumentException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.Map;

/**
 * Interface for a validation task run by validateEnv command.
 */
public interface ValidationTask {
  /**
   * @return options of this validation task
   */
  Options getOptions();

  /**
   * Parses the command line arguments and options in {@code args}.
   *
   * After successful execution of this method, command line arguments can be
   * retrieved by invoking {@link CommandLine#getArgs()}, and options can be
   * retrieved by calling {@link CommandLine#getOptions()}.
   *
   * @param args command line arguments to parse
   * @return {@link CommandLine} object representing the parsing result
   * @throws InvalidArgumentException if command line contains invalid argument(s)
   */
  CommandLine parseArgsAndOptions(String... args) throws InvalidArgumentException;

  /**
   * Runs the validation task.
   * @return whether the validation succeeds
   */
  boolean validate(Map<String, String> optionMap) throws InterruptedException;
}
