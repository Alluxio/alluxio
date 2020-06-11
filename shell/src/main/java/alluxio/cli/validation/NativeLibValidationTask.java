package alluxio.cli.validation;

import alluxio.cli.ValidationUtils;
import alluxio.conf.AlluxioConfiguration;

import java.io.File;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Validates the Java native libs defined in the system property.
 * */
@ApplicableUfsType(ApplicableUfsType.Type.ALL)
public class NativeLibValidationTask extends AbstractValidationTask {
  public static final String NATIVE_LIB_PATH = "java.library.path";
  public static final String NATIVE_LIB_PATH_SEPARATOR = ":";
  private final AlluxioConfiguration mConf;

  /**
   * Creates a new instance of {@link NativeLibValidationTask}
   * for validating Hadoop native lib path.
   * @param conf configuration
   */
  public NativeLibValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateJavaNativeLibPaths";
  }

  private ValidationUtils.TaskResult accessNativeLib() {
    // TODO(jiacheng): how do i get this property from mConf?
    String nativeLibPath = System.getProperty(NATIVE_LIB_PATH);
    StringTokenizer parser = new StringTokenizer(nativeLibPath, NATIVE_LIB_PATH_SEPARATOR);
    ValidationUtils.State state = ValidationUtils.State.OK;
    StringBuilder msg = new StringBuilder();
    msg.append(String.format("java.library.path=%s. ", nativeLibPath));
    StringBuilder advice = new StringBuilder();
    while (parser.hasMoreTokens()) {
      String path = parser.nextToken();
      File f = new File(path);
      if (!f.exists()) {
        state = ValidationUtils.State.WARNING;
        msg.append(String.format("Java native lib not found at %s.%n", path));
        advice.append(String.format("Please check your path %s.%n", path));
      }
    }
    return new ValidationUtils.TaskResult(state, getName(), msg.toString(), advice.toString());
  }

  @Override
  public ValidationUtils.TaskResult validate(Map<String, String> optionMap)
          throws InterruptedException {
    return accessNativeLib();
  }
}
