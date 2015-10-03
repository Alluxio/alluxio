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

package tachyon.client.lineage;

import java.io.IOException;

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.lineage.options.DeleteLineageOptions;
import tachyon.exception.LineageDeletionException;
import tachyon.exception.LineageDoesNotExistException;
import tachyon.exception.TachyonException;

/**
 * A LineageClient implementation. This class does not access the master client directly but goes
 * through the implementations provided in {@link AbstractLineageClient}.
 */
@PublicApi
public final class TachyonLineage extends AbstractLineageClient {
  private static TachyonLineage sTachyonLineage;

  public static synchronized TachyonLineage get() {
    if (sTachyonLineage == null) {
      if (!ClientContext.getConf().getBoolean(Constants.USER_LINEAGE_ENABLED)) {
        throw new IllegalStateException("Lineage is not enabled in the configuration.");
      }
      sTachyonLineage = new TachyonLineage();
    }
    return sTachyonLineage;
  }

  private TachyonLineage() {
    super();
  }

  /**
   * Convenience method for {@link #deleteLineage(long, DeleteLineageOptions)} with default options.
   */
  public boolean deleteLineage(long lineageId) throws IOException, LineageDoesNotExistException,
      LineageDeletionException, TachyonException {
    return super.deleteLineage(lineageId, DeleteLineageOptions.defaults());
  }
}
