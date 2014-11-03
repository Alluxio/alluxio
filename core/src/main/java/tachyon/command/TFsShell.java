package tachyon.command;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.io.Closer;

/**
 * Class for handling command line inputs.
 */
public class TFsShell implements Closeable {
   private static final String COMMAND_PACKAGE_PATH = "tachyon.command.commands.";
   private static final String COMMAND_NAME = "Command";
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
   * Method which prints the method to use all the commands.
   */
  public void printUsage() {
    System.out.println("Usage: java TFsShell");
    System.out.println("       [cat <path>]");
    System.out.println("       [count <path>]");
    System.out.println("       [ls <path>]");
    System.out.println("       [lsr <path>]");
    System.out.println("       [mkdir <path>]");
    System.out.println("       [rm <path>]");
    System.out.println("       [tail <path>]");
    System.out.println("       [touch <path>]");
    System.out.println("       [mv <src> <dst>]");
    System.out.println("       [copyFromLocal <src> <remoteDst>]");
    System.out.println("       [copyToLocal <src> <localDst>]");
    System.out.println("       [fileinfo <path>]");
    System.out.println("       [location <path>]");
    System.out.println("       [report <path>]");
    System.out.println("       [request <tachyonaddress> <dependencyId>]");
    System.out.println("       [pin <path>]");
    System.out.println("       [unpin <path>]");
  }

  /**
   * Method which determines how to handle the user's request, will display usage help to the user
   * if command format is incorrect.
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred
   */
  public int run(String[] argv) {
    int result = -1;
    AbstractCommands command = null;
    StringBuilder commandClass = new StringBuilder(COMMAND_PACKAGE_PATH)
      .append(argv[0].substring(0, 1).toUpperCase()).append(argv[0].substring(1))
      .append(COMMAND_NAME);
    try {
      command = (AbstractCommands) Class.forName(commandClass.toString()).newInstance();
      command.setmCloser(mCloser);
      result = command.execute(argv);
    } catch (IndexOutOfBoundsException e ) {
        printUsage();
    } catch (ClassNotFoundException e ) {
      printUsage();
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
    } finally {
      return result;
    }
  }
}