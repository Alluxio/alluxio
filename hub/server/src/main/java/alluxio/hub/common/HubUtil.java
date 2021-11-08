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

package alluxio.hub.common;

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.hub.proto.ValidationResult;
import alluxio.hub.proto.ValidationStatus;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * Hub Utility Class.
 */
public class HubUtil {
  private HubUtil() {} // prevent instantiation

  /**
   * Convert from ValidationTaskResult.
   * @param res result
   * @return Validation Result
   */
  public static ValidationResult convertFromValidationTaskResult(ValidationTaskResult res) {
    return ValidationResult.newBuilder()
        .setTestResult(ValidationStatus.forNumber(res.getState().ordinal()))
        .setAdvice(res.getAdvice())
        .setDescription(res.getDesc())
        .setName(res.getName())
        .setOutput(res.getResult())
        .build();
  }

  /**
   * Convert from ValidationResult map to a list of validation result list.
   * @param input input map
   * @return Validation Result list
   */
  public static List<ValidationResult> convertValidationResult(Map<ValidationUtils.State,
      List<ValidationTaskResult>> input) {
    return input.entrySet().stream()
        .flatMap(e -> e.getValue().stream()
            .map(HubUtil::convertFromValidationTaskResult))
        .collect(Collectors.toList());
  }

  /**
   * Get environment (key, value) pair from an environment file with export key = "value".
   * @param fileContent content of environment file
   * @param logger log4j output for logging
   * @return (key, value) map
   */
  public static Map<String, String> getEnvVarsFromFile(String fileContent, Logger logger) {
    HashMap<String, String> map = new HashMap<>();
    try (Scanner scanner = new Scanner(fileContent)) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        String[] keyVals = line.split("=", 2);
        if (keyVals.length != 2) {
          logger.warn("Invalid format while parsing line for environment variables: {}", line);
          continue;
        }
        String key = keyVals[0].trim();
        String value = keyVals[1].trim();
        if (key.startsWith("export ")) {
          map.put(key.substring("export ".length()).trim(), value.replace("\"", ""));
        }
      }
    }
    return map;
  }
}
