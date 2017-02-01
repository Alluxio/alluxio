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

import alluxio.Configuration;
import alluxio.PropertyKey;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Map.Entry;

/**
 * Utility for printing Alluxio configuration.
 */
public final class GetConf {
  private static final String USAGE = "USAGE: GetConf [-inBytes] [KEY]\n\n"
      + "GetConf [KEY] prints the configured value for the given key. If the key is invalid, the "
      + "exit code will be nonzero. If the key is valid but isn't set, an empty string is printed. "
      + "If no key is specified, all configuration is printed. If \"inBytes\" option is specified, "
      + "a value of \"1KB\" will be converted to a value of 1024.";

  private static final String BYTES_OPTION_NAME = "bytes";
  private static final Option BYTES_OPTION =
      Option.builder().required(false).longOpt(BYTES_OPTION_NAME).hasArg(false)
          .desc("value in bytes").build();

  /**
   * @param args should be size 0 or 1; if size 1, the specified configuration value is printed,
   *        otherwise all configuration is printed
   */
  public static void main(String[] args) {
    Options opts = new Options().addOption(BYTES_OPTION);
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(opts, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      System.err.println("Unable to parse input args: " + e.getMessage());
      System.out.println(USAGE);
      System.exit(1);
    }
    Preconditions.checkNotNull(cmd, "Unable to parse input args");
    args = cmd.getArgs();
    switch (args.length) {
      case 0:
        for (Entry<String, String> entry : Configuration.toMap().entrySet()) {
          String key = entry.getKey();
          String value = entry.getValue();
          System.out.println(String.format("%s=%s", key, value));
        }
        break;
      case 1:
        if (!PropertyKey.isValid(args[0])) {
          System.out.println(String.format("%s is not a valid configuration key", args[0]));
          System.out.println(USAGE);
          System.exit(1);
        }
        PropertyKey key = PropertyKey.fromString(args[0]);
        if (!Configuration.containsKey(key)) {
          System.out.println("");
        } else {
          if (cmd.hasOption(BYTES_OPTION_NAME)) {
            System.out.println(Configuration.getBytes(key));
          } else {
            System.out.println(Configuration.get(key));
          }
        }
        break;
      default:
        System.out.println(USAGE);
        System.exit(1);
    }
  }

  private GetConf() {} // this class is not intended for instantiation
}
