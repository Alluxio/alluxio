package tachyon.examples;

import java.io.IOException;

import tachyon.Constants;
import tachyon.client.ReadType;
import tachyon.client.WriteType;

public final class Utils {
  private Utils() {
  }

  public static void printPassInfo(boolean pass) {
    if (pass) {
      System.out.println(Constants.ANSI_GREEN + "Passed the test!" + Constants.ANSI_RESET);
    } else {
      System.out.println(Constants.ANSI_RED + "Failed the test!" + Constants.ANSI_RESET);
    }
  }

  public static String option(String[] args, int index, String defaultValue) {
    if (index < args.length && index >= 0) {
      return args[index];
    } else {
      return defaultValue;
    }
  }

  public static boolean option(String[] args, int index, boolean defaultValue) {
    if (index < args.length && index >= 0) {
      // if data isn't a boolean, false is returned here. Unable to check this.
      return Boolean.parseBoolean(args[index]);
    } else {
      return defaultValue;
    }
  }

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

  public static WriteType option(String[] args, int index, WriteType defaultValue) {
    if (index < args.length && index >= 0) {
      try {
        return WriteType.getOpType(args[index]);
      } catch (IOException e) {
        System.err.println("Unable to parse WriteType;" + e.getMessage());
        System.err.println("Defaulting to " + defaultValue);
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }

  public static ReadType option(String[] args, int index, ReadType defaultValue) {
    if (index < args.length && index >= 0) {
      try {
        return ReadType.getOpType(args[index]);
      } catch (IOException e) {
        System.err.println("Unable to parse ReadType;" + e.getMessage());
        System.err.println("Defaulting to " + defaultValue);
        return defaultValue;
      }
    } else {
      return defaultValue;
    }
  }
}
