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

import com.google.common.base.Objects;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Each file is viewed as a vector of sorted rows, then do a merge-sort of these vectors and
 * write the output rows to the compacted files, complete a compacted file when the file
 * reaches the expected size.
 */
public class HeapCompactor implements Compactor {
  private final Comparator<TableRow> mComparator;

  private static final class Input implements Comparable<Input> {
    private final Comparator<TableRow> mComparator;
    private final TableReader mReader;
    private TableRow mRow;

    public Input(TableReader reader, Comparator<TableRow> comparator) {
      mComparator = comparator;
      mReader = reader;
    }

    public TableRow getRow() {
      return mRow;
    }

    public boolean readNextRow() throws IOException {
      mRow = mReader.read();
      return mRow != null;
    }

    @Override
    public int compareTo(Input o) {
      return mComparator.compare(mRow, o.mRow);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }

      if (!(o instanceof Input)) {
        return false;
      }

      Input other = (Input) o;

      return compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mComparator, mReader, mRow);
    }
  }

  /**
   * @param comparator the comparator to compare rows' orders
   */
  public HeapCompactor(Comparator<TableRow> comparator) {
    mComparator = comparator;
  }

  @Override
  public void compact(List<TableReader> inputs, TableWriter output) throws IOException {
    PriorityQueue<Input> queue = new PriorityQueue<>();
    for (TableReader reader : inputs) {
      Input input = new Input(reader, mComparator);
      input.readNextRow();
      queue.add(input);
    }
    while (!queue.isEmpty()) {
      Input top = queue.poll();
      output.write(top.getRow());
      if (top.readNextRow()) {
        queue.add(top);
      }
    }
  }
}
