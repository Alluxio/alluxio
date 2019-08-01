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

package alluxio.client.catalog;

import alluxio.Client;
import alluxio.master.MasterClientContext;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.List;

/**
 * A client to use for interacting with a catalog master.
 */
@ThreadSafe
public interface CatalogMasterClient extends Client {

  /**
   * Factory for {@link CatalogMasterClient}.
   */
  class Factory {

    private Factory() {
    } // prevent instantiation

    /**
     * Factory method for {@link CatalogMasterClient}.
     *
     * @param conf master client configuration
     * @return a new {@link CatalogMasterClient} instance
     */
    public static CatalogMasterClient create(MasterClientContext conf) {
      return new RetryHandlingCatalogMasterClient(conf);
    }
  }

  List<String> getAllDatabase() throws IOException;
}

