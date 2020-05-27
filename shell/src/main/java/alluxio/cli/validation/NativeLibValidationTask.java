package alluxio.cli.validation;

import alluxio.conf.AlluxioConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class NativeLibValidationTask extends AbstractValidationTask {
  private final AlluxioConfiguration mConf;

  /**
   * Creates a new instance of {@link NativeLibValidationTask}
   * for validating Hadoop native lib path.
   * @param conf configuration
   */
  public NativeLibValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }


  private TaskResult accessNativeLib() {
    String taskName = "Acess native lib path";
    String nativeLibPath = System.getProperty("java.library.path");
    StringTokenizer parser = new StringTokenizer(nativeLibPath, ";");
    State state = State.OK;
    StringBuilder msg = new StringBuilder();
    msg.append(String.format("java.library.path=%s. ", nativeLibPath));
    StringBuilder advice = new StringBuilder();
    while (parser.hasMoreTokens()) {
      String path = parser.nextToken();
      File f = new File(path);
      if (!f.exists()) {
        state = State.WARNING;
        msg.append(String.format("Java native lib not found at %s. ", path));
        advice.append(String.format("Please check %s. ", path));
      }
    }
    return new TaskResult(state, taskName, msg.toString(), advice.toString());
  }

  @Override
  public State validate(Map<String, String> optionMap) throws InterruptedException {
    List<TaskResult> results = new ArrayList<>();
    results.add(accessNativeLib());
    return results;
  }
}
