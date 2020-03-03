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

import alluxio.ClientContext;
import alluxio.annotation.PublicApi;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.master.MasterClientContext;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Utility for printing Alluxio configuration.
 */
@PublicApi
public final class GetConf {
  private static final String USAGE =
      "USAGE: GetConf [--unit <arg>] [--source] [--master] [key]\n\n"
      + "GetConf prints the configured value for the given key. If the key is invalid, the "
      + "exit code will be nonzero. If the key is valid but isn't set, an empty string is printed. "
      + "If no key is specified, all configuration is printed.";

  private static final String MASTER_OPTION_NAME = "master";
  private static final String SOURCE_OPTION_NAME = "source";
  private static final String UNIT_OPTION_NAME = "unit";
  private static final Option MASTER_OPTION =
      Option.builder().required(false).longOpt(MASTER_OPTION_NAME).hasArg(false)
          .desc("the configuration properties used by the master.").build();
  private static final Option SOURCE_OPTION =
      Option.builder().required(false).longOpt(SOURCE_OPTION_NAME).hasArg(false)
          .desc("source of the configuration property will be printed.").build();
  private static final Option UNIT_OPTION =
      Option.builder().required(false).longOpt(UNIT_OPTION_NAME).hasArg(true)
          .desc("unit of the value to return. Values of configuration will be converted "
              + "to a quantity in the given unit. E.g., with \"--unit KB\", a configuration value "
              + "of \"4096B\" will return 4, and with \"--unit S\", a configuration value of "
              + "\"5000ms\" will return 5. Possible unit options include B, KB, MB, GB, TP, PB, "
              + "MS, S, M, H, D. \"").build();
  private static final Options OPTIONS =
      new Options().addOption(SOURCE_OPTION).addOption(UNIT_OPTION).addOption(MASTER_OPTION);

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
   * @param ctx Alluxio client configuration
   * @param args list of arguments
   * @return 0 on success, 1 on failures
   */
  public static int getConf(ClientContext ctx, String... args) {
    return getConfImpl(
        () -> new RetryHandlingMetaMasterConfigClient(MasterClientContext.newBuilder(ctx).build()),
        ctx.getClusterConf(), args);
  }

  /**
   * Implements get configuration.
   *
   * @param clientSupplier a functor to return a config client of meta master
   * @param alluxioConf Alluxio configuration
   * @param args list of arguments
   * @return 0 on success, 1 on failures
   */
  @VisibleForTesting
  public static int getConfImpl(Supplier<RetryHandlingMetaMasterConfigClient> clientSupplier,
      AlluxioConfiguration alluxioConf, String... args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      printHelp("Unable to parse input args: " + e.getMessage());
      return 1;
    }
    args = cmd.getArgs();

    Map<String, ConfigProperty> confMap = new HashMap<>();
    if (cmd.hasOption(MASTER_OPTION_NAME)) {
      // load cluster-wide configuration
      try (RetryHandlingMetaMasterConfigClient client = clientSupplier.get()) {
        client.getConfiguration(GetConfigurationPOptions.newBuilder()
            .setIgnorePathConf(true).build()).getClusterConf().forEach(
              prop -> confMap.put(prop.getName(), prop.toProto()));
      } catch (IOException e) {
        System.out.println("Unable to get master-side configuration: " + e.getMessage());
        return -1;
      }
    } else {
      // load local configuration
      for (PropertyKey key : alluxioConf.keySet()) {
        if (key.isBuiltIn()) {
          ConfigProperty.Builder config = ConfigProperty.newBuilder().setName(key.getName())
              .setSource(alluxioConf.getSource(key).toString());
          String val = alluxioConf.getOrDefault(key, null,
              ConfigurationValueOptions.defaults().useDisplayValue(true));
          if (val != null) {
            config.setValue(val);
          }
          confMap.put(key.getName(), config.build());
        }
      }
    }

    StringBuilder output = new StringBuilder();
    switch (args.length) {
      case 0:
        List<ConfigProperty> properties = new ArrayList<>(confMap.values());
        properties.sort(Comparator.comparing(ConfigProperty::getName));
        for (ConfigProperty property : properties) {
          String value = ConfigurationUtils.valueAsString(property.getValue());
          output.append(String.format("%s=%s", property.getName(), value));
          if (cmd.hasOption(SOURCE_OPTION_NAME)) {
            output.append(String.format(" (%s)", property.getSource()));
          }
          output.append("\n");
        }
        System.out.print(output.toString());
        break;
      case 1:
        if (!PropertyKey.isValid(args[0])) {
          printHelp(String.format("%s is not a valid configuration key", args[0]));
          return 1;
        }
        // args[0] can be the alias
        String key = PropertyKey.fromString(args[0]).getName();
        ConfigProperty property = confMap.get(key);
        if (property == null) {
          printHelp(String.format("%s is not found", key));
          return 1;
        }

        if (property.getValue() == null) {
          // value not set
          System.out.println("");
        } else {
          if (cmd.hasOption(SOURCE_OPTION_NAME)) {
            System.out.println(property.getSource());
          } else if (cmd.hasOption(UNIT_OPTION_NAME)) {
            String arg = cmd.getOptionValue(UNIT_OPTION_NAME).toUpperCase();
            try {
              ByteUnit byteUnit;
              byteUnit = ByteUnit.valueOf(arg);
              System.out.println(
                  FormatUtils.parseSpaceSize(property.getValue()) / byteUnit.getValue());
              break;
            } catch (Exception e) {
              // try next unit parse
            }
            try {
              TimeUnit timeUnit;
              timeUnit = TimeUnit.valueOf(arg);
              System.out.println(
                  FormatUtils.parseTimeSize(property.getValue()) / timeUnit.getValue());
              break;
            } catch (IllegalArgumentException ex) {
              // try next unit parse
            }
            printHelp(String.format("%s is not a valid unit", arg));
            return 1;
          } else {
            System.out.println(property.getValue());
          }
        }
        break;
      default:
        printHelp(String.format("More arguments than expected. Args: %s", Arrays.toString(args)));
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
    System.exit(getConf(
        ClientContext.create(new InstancedConfiguration(ConfigurationUtils.defaults())), args));
  }

  private GetConf() {} // this class is not intended for instantiation
}
