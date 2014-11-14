package tachyon.command;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.io.Closer;

import org.apache.commons.cli.*;

import tachyon.command.commands.CatCommand;
import tachyon.command.commands.CopyFromLocalCommand;
import tachyon.command.commands.CopyToLocalCommand;
import tachyon.command.commands.CountCommand;
import tachyon.command.commands.LocationCommand;
import tachyon.command.commands.LsCommand;
import tachyon.command.commands.LsrCommand;
import tachyon.command.commands.MkdirCommand;
import tachyon.command.commands.PinCommand;
import tachyon.command.commands.RenameCommand;
import tachyon.command.commands.ReportCommand;
import tachyon.command.commands.RequestCommand;
import tachyon.command.commands.RmCommand;
import tachyon.command.commands.TailCommand;
import tachyon.command.commands.TouchCommand;
import tachyon.command.commands.UnpinCommand;
import tachyon.command.commands.FileinfoCommand;


/**
 * Class for handling command line inputs.
 */
public class TFsShell implements Closeable {
  private static final String COMMAND_PACKAGE_PATH = "tachyon.command.commands.";
  private static final String COMMAND_NAME = "Command";
  private static final String PATH = "path";

  /**
   * Main method, starts a new TFsShell
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    TFsShell shell = new TFsShell();
    int ret;
    try {
      ret = shell.run(argv);
    } finally {
      shell.close();
    }
    System.exit(ret);
  }

  private final Closer mCloser = Closer.create();

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  /**
   * Method which determines how to handle the user's request, will display usage help to the user
   * if command format is incorrect.
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred
   */
  public int run(String[] argv) {
    //.Init
    int result = -1;
    AbstractCommands command = null;
    StringBuilder commandClass = null;
    Options options = null;
    CommandLine commandLine = null;
    try {
      //00.Construct options and commandLine
      options = constructOptions();
      commandLine = constructCommandLine(options, argv);
      //01.Get command class string
      commandClass = new StringBuilder(COMMAND_PACKAGE_PATH)
              .append(commandLine.getOptions()[0].getOpt().substring(0, 1).toUpperCase())
              .append(commandLine.getOptions()[0].getOpt().substring(1)).append(COMMAND_NAME);
      //02.Get command object and set closer
      command = (AbstractCommands) Class.forName(commandClass.toString()).newInstance();
      command.setmCloser(mCloser);
      //03. Execute
      result = command.execute(commandLine);
    } catch (IndexOutOfBoundsException e ) {
      printHelp(options);
    } catch (ClassNotFoundException e ) {
      printHelp(options);
    } catch (ParseException e) {
      printHelp(options, e.getMessage());
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
    } finally {
      return result;
    }
  }

  /**
   * Parse array of arguments in options.
   *
   * @param options Possible options in command
   * @param args [] Array of arguments given by the user's input from the terminal
   * @return CommandLine object with parse
   */
  private CommandLine constructCommandLine(Options options, String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      //.Add '-' to CLI parse
      args[0] = "-" + args[0];
      cmd = parser.parse(options, args, true);
      //.Drop '-' to test. 'args' var is not required anymore
      args[0] = args[0].replace("-", "");
    } catch (ParseException e) {
      throw e;
    }
    return cmd;
  }

  /**
   * Generate global options with arguments.
   *
   * @return Options for commands
   */
  private Options constructOptions() {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName(PATH).hasArg()
            .withDescription(CatCommand.DESCRIPTION)
            .create(CatCommand.NAME));
    options.addOption(OptionBuilder.withArgName(PATH).hasArg()
            .withDescription(CountCommand.DESCRIPTION)
            .create(CountCommand.NAME));
    options.addOption(OptionBuilder.withArgName(PATH).hasArg()
            .withDescription(LsCommand.DESCRIPTION)
            .create(LsCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName(PATH).hasArg()
            .withDescription(LsrCommand.DESCRIPTION)
            .create(LsrCommand.NAME));
    options.addOption(OptionBuilder.withArgName(PATH).hasArg()
            .withDescription(MkdirCommand.DESCRIPTION)
            .create(MkdirCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName(PATH).hasArg()
            .withDescription(RmCommand.DESCRIPTION)
            .create(RmCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName(PATH).hasArg()
            .withDescription(TailCommand.DESCRIPTION)
            .create(TailCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName(PATH).hasArg()
            .withDescription(TouchCommand.DESCRIPTION)
            .create(TouchCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName("src> <remoteDst").hasArgs(2)
            .withDescription(CopyFromLocalCommand.DESCRIPTION)
            .create(CopyFromLocalCommand.NAME));
    options.addOption(OptionBuilder.withArgName("src> <localDst").hasArgs(2).withDescription(
            CopyToLocalCommand.DESCRIPTION).create(CopyToLocalCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName(PATH).hasArg()
            .withDescription(FileinfoCommand.DESCRIPTION)
            .create(FileinfoCommand.NAME));
    options.addOption(OptionBuilder.withArgName(PATH).hasArg().withDescription(
            LocationCommand.DESCRIPTION).create(LocationCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName(PATH).hasArg()
            .withDescription(ReportCommand.DESCRIPTION)
            .create(ReportCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName("tachyonaddress> <dependencyId").hasArgs(2)
            .withDescription(RequestCommand.DESCRIPTION)
            .create(RequestCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName(PATH).hasArg()
            .withDescription(PinCommand.DESCRIPTION)
            .create(PinCommand.NAME));
    options.addOption(OptionBuilder
              .withArgName("src> <dst").hasArgs(2)
              .withDescription(RenameCommand.DESCRIPTION)
              .create(RenameCommand.NAME));
    options.addOption(OptionBuilder
            .withArgName(PATH).hasArg()
            .withDescription(UnpinCommand.DESCRIPTION)
            .create(UnpinCommand.NAME));
    return options;
  }

  /**
   * Print help/usage for tfs command
   *
   * @param options Options to formatter
   */
  private void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptPrefix("");
    formatter.printHelp("tfs <command>", options);
  }

  /**
   * Print help/usage for tfs command
   *
   * @param options Options to formatter
   */
  private void printHelp(Options options, String message) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptPrefix("");

    Option o = options.getOption(message.substring(message.lastIndexOf(':')+1).trim());
    formatter.printHelp(" ", new Options().addOption(o));
  }
}