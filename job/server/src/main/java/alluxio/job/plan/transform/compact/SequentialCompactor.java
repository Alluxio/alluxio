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

package alluxio.job.plan.transform.compact;

import alluxio.job.plan.transform.format.TableReader;
import alluxio.job.plan.transform.format.TableRow;
import alluxio.job.plan.transform.format.TableWriter;

import java.io.IOException;
import java.util.List;

/**
 * Sequentially read through files listed directly under the directory, complete a compacted
 * file when the file reaches the expected size.
 */
public class SequentialCompactor implements Compactor {
  @Override
  public void compact(List<TableReader> inputs, TableWriter output) throws IOException {
    for (TableReader input : inputs) {
      for (TableRow row = input.read(); row != null; row = input.read()) {
        output.write(row);
      }
    }
  }
}
