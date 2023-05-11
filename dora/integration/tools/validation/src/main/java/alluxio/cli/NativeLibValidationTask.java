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

  /**
   * Creates a new instance of {@link NativeLibValidationTask}
   * for validating Hadoop native lib path.
   */
  public NativeLibValidationTask() {}

  @Override
  public String getName() {
    return "ValidateJavaNativeLibPaths";
  }

  private ValidationTaskResult accessNativeLib() {
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
    return new ValidationTaskResult(state, getName(), msg.toString(), advice.toString());
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionMap)
          throws InterruptedException {
    return accessNativeLib();
  }
}
