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

import alluxio.cli.docgen.ConfigurationDocGenerator;
import alluxio.cli.docgen.MetricsDocGenerator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

/**
 * Utility for generating docs.
 */
public class DocGenerator {
  private static final String USAGE =
      "USAGE: DocGenerator [--metric] [--conf]\n\n"
          + "DocGenerator generates the docs for metric keys and/or property keys. "
          + "It will generate all the docs by default unless --metric or --conf is given.";

  private static final String METRIC_OPTION_NAME = "metric";
  private static final String CONF_OPTION_NAME = "conf";

  private static final Option METRIC_OPTION =
      Option.builder().required(false).longOpt(METRIC_OPTION_NAME).hasArg(false)
          .desc("the configuration properties used by the master.").build();
  private static final Option CONF_OPTION =
      Option.builder().required(false).longOpt(CONF_OPTION_NAME).hasArg(false)
          .desc("the configuration properties used by the master.").build();

  private static final Options OPTIONS =
      new Options().addOption(METRIC_OPTION).addOption(CONF_OPTION);

  /**
   * Main entry for this util class.
   *
   * @param args arguments for command line
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 0) {
      CommandLineParser parser = new DefaultParser();
      CommandLine cmd;
      try {
        cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
      } catch (ParseException e) {
        printHelp("Unable to parse input args: " + e.getMessage());
        return;
      }
      if (cmd.hasOption(METRIC_OPTION_NAME)) {
        MetricsDocGenerator.generate();
      }
      if (cmd.hasOption(CONF_OPTION_NAME)) {
        ConfigurationDocGenerator.generate();
      }
    } else {
      MetricsDocGenerator.generate();
      ConfigurationDocGenerator.generate();
    }
  }

  /**
   * Prints the help message.
   *
   * @param message message before standard usage information
   */
  public static void printHelp(String message) {
    System.err.println(message);
    HelpFormatter help = new HelpFormatter();
    help.printHelp(USAGE, OPTIONS);
  }

  private DocGenerator() {} // prevent instantiation
}
