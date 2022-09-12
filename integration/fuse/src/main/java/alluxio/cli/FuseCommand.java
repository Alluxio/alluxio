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

import alluxio.AlluxioURI;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.fuse.FuseMetadataSystem.FuseURIStatus;

/**
 * An interface for all fuse shell commands.
 */
public interface FuseCommand extends Command {
  /**
   * Checks if the arguments are valid or throw InvalidArgumentException.
   *
   * @param argv args need to be validated
   * @throws InvalidArgumentException when arguments are not valid
   */
  default void validateArgs(String[] argv) throws InvalidArgumentException {}

  /**
   * Runs the command.
   *
   * @param path the path uri from fuse command
   * @param argv args from fuse command
   * @return the result of running the command
   */
  default FuseURIStatus run(AlluxioURI path, String[] argv) throws InvalidArgumentException {}
}
