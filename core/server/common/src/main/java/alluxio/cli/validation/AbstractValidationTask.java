package alluxio.cli.validation;

import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Abstract class for validation environment.
 */
public abstract class AbstractValidationTask implements ValidationTask {
  protected static final Option HADOOP_CONF_DIR_OPTION =
      Option.builder("hadoopConfDir").required(false).hasArg(true)
      .desc("path to server-side hadoop conf dir").build();

  /**
   * {@inheritDoc}
   */
  @Override
  public Options getOptions() {
    return new Options();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CommandLine parseArgsAndOptions(String... args) throws InvalidArgumentException {
    Options opts = getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(opts, args);
    } catch (ParseException e) {
      throw new InvalidArgumentException(
          "Failed to parse args for validateEnv", e);
    }
    return cmd;
  }
}
