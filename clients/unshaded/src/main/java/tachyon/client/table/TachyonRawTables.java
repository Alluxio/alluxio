/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.exception.TachyonException;

/**
 * An implementation of Tachyon Raw Table client. This is simply a wrapper around
 * {@link AbstractTachyonRawTables}. Users can obtain an instance of this class by calling
 * {@link TachyonRawTablesFactory#get()}.
 */
@PublicApi
public final class TachyonRawTables extends AbstractTachyonRawTables {
  private static TachyonRawTables sTachyonRawTables;

  /**
   * The factory for the {@link TachyonRawTables}.
   */
  public static final class TachyonRawTablesFactory {
    private TachyonRawTablesFactory() {} // prevent init

    /**
     * @return the current {@link TachyonRawTables} instance
     */
    public static synchronized TachyonRawTables get() {
      if (sTachyonRawTables == null) {
        sTachyonRawTables = new TachyonRawTables();
      }
      return sTachyonRawTables;
    }
  }

  private TachyonRawTables() {
    super();
  }

  /**
   * Convenience method for {@link TachyonRawTables#create(TachyonURI, int, ByteBuffer)} with
   * empty metadata.
   *
   * @param path the path of the table to create in Tachyon space, this must not already exist as
   *             a file or table.
   * @param numColumns the number of columns in the table
   * @return a {@link RawTable} satisfying the input parameters
   * @throws IOException if a non Tachyon related I/O error occurs
   * @throws TachyonException if an internal Tachyon error occurs
   */
  public RawTable create(TachyonURI path, int numColumns) throws IOException, TachyonException {
    return create(path, numColumns, ByteBuffer.allocate(0));
  }
}
