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

package alluxio.cli.docgen;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

import java.util.List;

/**
 * Doc validation abstract class to define common methods and constants.
 */
abstract class DocValidator {
  public static final String CSV_FILE_DIR = "docs/_data/table/";
  public static final String YML_FILE_DIR = "docs/_data/table/en/";
  public static final String CSV_SUFFIX = "csv";
  public static final String YML_SUFFIX = "yml";

  public static Integer generateDiff(List<String> original, List<String> revised,
                                        String fileFullPathName) {
    // Compute diff. Get the Patch object. Patch is the container for computed deltas.
    Patch patch = DiffUtils.diff(original, revised);
    List<Delta> deltas = patch.getDeltas();

    if (deltas.size() > 0) {
      List<String> unifiedDiff =
            DiffUtils.generateUnifiedDiff(fileFullPathName, "revised", original, patch, 3);
      System.out.println("");
      for (String line : unifiedDiff) {
        System.out.println(line);
      }
    }
    return deltas.size();
  }
}
