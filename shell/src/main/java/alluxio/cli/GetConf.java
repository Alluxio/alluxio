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
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Map.Entry;

/**
 * Utility for printing Alluxio configuration.
 */
public final class GetConf {
  private static final String USAGE = "USAGE: GetConf [--unit <arg>] [KEY]\n\n"
      + "GetConf prints the configured value for the given key. If the key is invalid, the "
      + "exit code will be nonzero. If the key is valid but isn't set, an empty string is printed. "
      + "If no key is specified, all configuration is printed. If \"--unit\" option is specified, "
      + "values of data size configuration will be converted to a quantity in the given unit."
      + "E.g., with \"--unit KB\", a configuration value of \"4096\" will return 4, "
      + "and with \"--unit S\", a configuration value of \"5000\" will return 5."
      + "Possible unit options include B, KB, MB, GB, TP, PB, MS, S, M, H, D";

  private static final String UNIT_OPTION_NAME = "unit";
  private static final Option UNIT_OPTION =
      Option.builder().required(false).longOpt(UNIT_OPTION_NAME).hasArg(true)
          .desc("unit of the value to return.").build();
  private static final Options OPTIONS = new Options().addOption(UNIT_OPTION);

  private enum ByteUnit {
    B(1L),
    KB(1L << 10),
    MB(1L << 20),
    GB(1L << 30),
    TB(1L << 40),
    PB(1L << 50);

    /** value associated with each unit. */
    private long mValue;

    /**
     * @return the value of this unit
     */
    public long getValue() {
      return mValue;
    }

    ByteUnit(long value) {
      mValue = value;
    }
  }

  private enum TimeUnit {
    MS(1L),
    MILLISECOND(1L),
    S(1000L),
    SEC(1000L),
    SECOND(1000L),
    M(60000L),
    MIN(60000L),
    MINUTE(60000L),
    H(3600000L),
    HR(3600000L),
    HOUR(3600000L),
    D(86400000L),
    DAY(86400000L);

    /** value associated with each unit. */
    private long mValue;

    /**
     * @return the value of this unit
     */
    public long getValue() {
      return mValue;
    }

    TimeUnit(long value) {
      mValue = value;
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

  /**
   * Implements get configuration.
   *
   * @param args list of arguments
   * @return 0 on success, 1 on failures
   */
  public static int getConf(String... args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      printHelp("Unable to parse input args: " + e.getMessage());
      return 1;
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
          printHelp(String.format("%s is not a valid configuration key", args[0]));
          return 1;
        }
        PropertyKey key = PropertyKey.fromString(args[0]);
        if (!Configuration.containsKey(key)) {
          System.out.println("");
        } else {
          if (cmd.hasOption(UNIT_OPTION_NAME)) {
            String arg = cmd.getOptionValue(UNIT_OPTION_NAME).toUpperCase();
            try {
              TimeUnit timeUnit;
              timeUnit = TimeUnit.valueOf(arg);
              System.out.println(Configuration.getMs(key) / timeUnit.getValue());
              break;
            } catch (IllegalArgumentException ex) {
              // try next unit parse
            }
            try {
              ByteUnit byteUnit;
              byteUnit = ByteUnit.valueOf(arg);
              System.out.println(Configuration.getBytes(key) / byteUnit.getValue());
              break;
            } catch (Exception e) {
              // try next unit parse
            }
            printHelp(String.format("%s is not a valid unit", arg));
            return 1;
          } else {
            System.out.println(Configuration.get(key));
          }
        }
        break;
      default:
        printHelp("More arguments than expected");
        return 1;
    }
    return 0;
  }

  /**
   * Prints Alluxio configuration.
   *
   * @param args the arguments to specify the unit (optional) and configuration key (optional)
   */
  public static void main(String[] args) {
    System.exit(getConf(args));
  }

  private GetConf() {} // this class is not intended for instantiation
}
