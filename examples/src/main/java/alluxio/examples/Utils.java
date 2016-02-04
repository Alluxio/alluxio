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

package alluxio.examples;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.TachyonStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;

/**
 * Utilities to run the examples.
 */
public final class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private Utils() {}

  /**
   * Prints information of the test result.
   *
   * @param pass the test result
   */
  public static void printPassInfo(boolean pass) {
    if (pass) {
      System.out.println(Constants.ANSI_GREEN + "Passed the test!" + Constants.ANSI_RESET);
    } else {
      System.out.println(Constants.ANSI_RED + "Failed the test!" + Constants.ANSI_RESET);
    }
  }

  /**
   * Provides the options to show in the usage for a {@link String}.
   *
   * @param args the arguments to parse
   * @param index the index of the option
   * @param defaultValue the default value
   * @return either the value of the index of the arguments or the default value
   */
  public static String option(String[] args, int index, String defaultValue) {
    if (index < args.length && index >= 0) {
      return args[index];
    } else {
      return defaultValue;
    }
  }

  /**
   * Provides the options to show in the usage for a {@code boolean}.
   *
   * @param args the arguments to parse
   * @param index the index of the option
   * @param defaultValue the default value
   * @return either the value of the index of the arguments or the default value
   */
  public static boolean option(String[] args, int index, boolean defaultValue) {
    if (index < args.length && index >= 0) {
      // if data isn't a boolean, false is returned here. Unable to check this.
      return Boolean.parseBoolean(args[index]);
    } else {
      return defaultValue;
    }
  }

  /**
   * Provides the options to show in the usage for an {@code int}.
   *
   * @param args the arguments to parse
   * @param index the index of the option
   * @param defaultValue the default value
   * @return either the value of the index of the arguments or the default value
   */
  public static int option(String[] args, int index, int defaultValue) {
    if (index < args.length && index >= 0) {
      try {
        return Integer.parseInt(args[index]);
      } catch (NumberFormatException e) {
        System.err.println("Unable to parse int;" + e.getMessage());
        System.err.println("Defaulting to " + defaultValue);
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  /**
   * Provides the options to show in the usage for a {@link ReadType}.
   *
   * @param args the arguments to parse
   * @param index the index of the option
   * @param defaultValue the default value
   * @return either the value of the index of the arguments or the default value
   */
  public static ReadType option(String[] args, int index, ReadType defaultValue) {
    if (index < args.length && index >= 0) {
      try {
        return ReadType.valueOf(args[index]);
      } catch (IllegalArgumentException e) {
        System.err.println("Unable to parse ReadType;" + e.getMessage());
        System.err.println("Defaulting to " + defaultValue);
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  /**
   * Provides the options to show in the usage for a {@link TachyonStorageType}.
   *
   * @param args the arguments to parse
   * @param index the index of the option
   * @param defaultValue the default value
   * @return either the value of the index of the arguments or the default value
   */
  public static TachyonStorageType option(String[] args, int index,
      TachyonStorageType defaultValue) {
    if (index < args.length && index >= 0) {
      try {
        return TachyonStorageType.valueOf(args[index]);
      } catch (IllegalArgumentException e) {
        System.err.println("Unable to parse TachyonStorageType;" + e.getMessage());
        System.err.println("Defaulting to " + defaultValue);
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  /**
   * Provides the options to show in the usage for a {@link UnderStorageType}.
   *
   * @param args the arguments to parse
   * @param index the index of the option
   * @param defaultValue the default value
   * @return either the value of the index of the arguments or the default value
   */
  public static UnderStorageType option(String[] args, int index, UnderStorageType defaultValue) {
    if (index < args.length && index >= 0) {
      try {
        return UnderStorageType.valueOf(args[index]);
      } catch (IllegalArgumentException e) {
        System.err.println("Unable to parse UnderStorageType;" + e.getMessage());
        System.err.println("Defaulting to " + defaultValue);
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  /**
   * Provides the options to show in the usage for a {@link WriteType}.
   *
   * @param args the arguments to parse
   * @param index the index of the option
   * @param defaultValue the default value
   * @return either the value of the index of the arguments or the default value
   */
  public static WriteType option(String[] args, int index, WriteType defaultValue) {
    if (index < args.length && index >= 0) {
      try {
        return WriteType.valueOf(args[index]);
      } catch (IllegalArgumentException e) {
        System.err.println("Unable to parse WriteType;" + e.getMessage());
        System.err.println("Defaulting to " + defaultValue);
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  /**
   * Runs an example.
   *
   * @param example the example to run
   */
  public static void runExample(final Callable<Boolean> example) {
    boolean result;
    try {
      result = example.call();
    } catch (Exception e) {
      LOG.error("Exception running test: " + example, e);
      result = false;
    }
    Utils.printPassInfo(result);
    System.exit(result ? 0 : 1);
  }
}
