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

import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for running tests against an existing hive metastore.
 */
public class HmsTests {
  private static final String HELP_OPTION_NAME = "h";

  private static final Option HELP_OPTION =
      Option.builder(HELP_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("print help information.")
          .build();

  private static final Option METASTORE_URI_OPTION =
      Option.builder(ValidationConfig.METASTORE_URI_OPTION_NAME)
          .required(true)
          .hasArg(true)
          .desc("Uri(s) to connect to hive metastore.")
          .build();

  private static final Option DATABASE_OPTION =
      Option.builder(ValidationConfig.DATABASE_OPTION_NAME)
          .required(false)
          .hasArg(true)
          .desc("Database to run tests against.")
          .build();
  private static final Option TABLES_OPTION =
      Option.builder(ValidationConfig.TABLES_OPTION_NAME)
          .required(false)
          .hasArg(true)
          .desc("Tables to run tests against. Multiple tables should be separated with comma.")
          .build();
  private static final Option SOCKET_TIMEOUT_OPTION =
      Option.builder(ValidationConfig.SOCKET_TIMEOUT_OPTION_NAME)
          .required(false)
          .hasArg(true)
          .desc("Socket timeout of hive metastore client in minutes. "
              + "Consider increasing this if you have tables with a lot of metadata.")
          .build();

  private static final Options OPTIONS =
      new Options().addOption(HELP_OPTION).addOption(METASTORE_URI_OPTION)
          .addOption(DATABASE_OPTION).addOption(TABLES_OPTION).addOption(SOCKET_TIMEOUT_OPTION);

  private static void printUsage() {
    new HelpFormatter().printHelp("alluxio runHmsTests",
        "Test the configuration, connectivity, and permission "
            + "of an existing hive metastore", OPTIONS,
        "e.g. options '-m thrift://hms_host:9083 -d tpcds -t store_sales,web_sales -st 20'"
            + "will connect to the hive metastore and run tests against the given database "
            + "and tables.", true);
  }

  /**
   * @param args the input arguments
   */
  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      printUsage();
      System.exit(1);
    }

    if (cmd.hasOption(HELP_OPTION_NAME)) {
      printUsage();
      return;
    }
    String metastoreUri = cmd.getOptionValue(ValidationConfig.METASTORE_URI_OPTION_NAME);
    String database = cmd.getOptionValue(ValidationConfig.DATABASE_OPTION_NAME, "");
    String tables = cmd.getOptionValue(ValidationConfig.TABLES_OPTION_NAME, "");
    int socketTimeout = Integer.parseInt(cmd
        .getOptionValue(ValidationConfig.SOCKET_TIMEOUT_OPTION_NAME, "-1"));

    ValidationToolRegistry registry
        = new ValidationToolRegistry(new InstancedConfiguration(ConfigurationUtils.defaults()));
    // Load hms validation tool from alluxio lib directory
    registry.refresh();

    Map<Object, Object> configMap = new HashMap<>();
    configMap.put(ValidationConfig.METASTORE_URI_CONFIG_NAME, metastoreUri);
    configMap.put(ValidationConfig.DATABASE_CONFIG_NAME, database);
    configMap.put(ValidationConfig.TABLES_CONFIG_NAME, tables);
    configMap.put(ValidationConfig.SOCKET_TIMEOUT_CONFIG_NAME, socketTimeout);

    ValidationTool tests = registry.create(ValidationConfig.HMS_TOOL_TYPE, configMap);
    String result = ValidationTool.toJson(ValidationTool.convertResults(tests.runAllTests()));
    System.out.println(result);
    if (result.contains(ValidationUtils.State.FAILED.toString())) {
      System.exit(-1);
    }
    System.exit(0);
  }
}
