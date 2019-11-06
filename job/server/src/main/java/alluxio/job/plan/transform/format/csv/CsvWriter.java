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

package alluxio.job.plan.transform.format.csv;

import alluxio.job.plan.transform.format.TableRow;
import alluxio.job.plan.transform.format.TableWriter;

import java.io.IOException;

/**
 * A writer for writing {@link CsvRow}.
 */
public final class CsvWriter implements TableWriter {
  @Override
  public void write(TableRow row) throws IOException {
    // TODO(cc)
  }

  @Override
  public void close() throws IOException {
    // TODO(cc)
  }

  @Override
  public int getRows() {
    // TODO(cc)
    return 0;
  }

  @Override
  public long getBytes() {
    // TODO(cc)
    return 0;
  }
}
