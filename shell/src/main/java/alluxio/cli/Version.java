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

import alluxio.ProjectConstants;
import alluxio.RuntimeConstants;
import alluxio.annotation.PublicApi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The version of the current build.
 */
@ThreadSafe
@PublicApi
public final class Version {

  private static final Option REVISION_OPT = Option.builder("r")
      .longOpt("revision")
      .desc("Prints the git revision along with the Alluxio version. Optionally specify the "
          + "revision length")
      .optionalArg(true)
      .argName("revision_length")
      .hasArg(true)
      .build();

  private Version() {} // prevent instantiation

  /**
   * Prints the version of the current build.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    CommandLineParser parser = new DefaultParser();
    Options opts = new Options().addOption(REVISION_OPT);
    try {
      CommandLine cl = parser.parse(opts, args);
      String revision = "";
      if (cl.hasOption(REVISION_OPT.getOpt())) {
        String r = cl.getOptionValue(REVISION_OPT.getOpt());
        int revisionLen = r == null ? ProjectConstants.REVISION.length() :
            Integer.parseInt(r);
        revision = String.format("-%s", ProjectConstants.REVISION.substring(0,
            Math.min(revisionLen, ProjectConstants.REVISION.length())));
      }
      System.out.println(String.format("%s%s", RuntimeConstants.VERSION, revision));
      System.exit(0);
    } catch (ParseException e) {
      printUsage(opts);
    } catch (NumberFormatException e) {
      System.out.println(String.format("Couldn't parse <%s> as an integer",
          REVISION_OPT.getValue()));
      printUsage(opts);
    }
    System.exit(1);
  }

  /**
   * Prints the usage of the version command.
   *
   * @param options the set of command line options to put in the help text
   */
  public static void printUsage(Options options) {
    new HelpFormatter().printHelp("version", options);
  }
}
