package alluxio.cli.fuse;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.ParseException;

/**
 * This class is the entry class for fuse correctness validation tool.
 */
public class FuseCorrectnessValidation {
  /**
   * This main functiona is the entry point for fuse correctness validation.
   * @param args command line args
   */
  public static void main(String[] args) {
    CorrectnessValidationOptions options = null;
    try {
      options = CorrectnessValidationOptions.createOptions(args);
    } catch (ParseException e) {
      System.out.println("Unexpected error while parsing command line." + e);
      System.exit(1);
    } catch (IllegalArgumentException e) {
      System.out.println("Testing operation invalid." + e);
      System.exit(1);
    }
    Preconditions.checkNotNull(options,
        "Command line options object should not be null after parsing.");

    switch (options.getOperation()) {
      case READ:
        ValidateRead.validateReadCorrectness(options);
        break;
      case WRITE:
        ValidateWrite.validateWriteCorrectness(options);
        break;
      default:
        break;
    }
  }
}
