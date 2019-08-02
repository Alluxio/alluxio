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

package alluxio.experimental;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Experimental module wide constants.
 */
@ThreadSafe
public final class Constants {
  public static final String CATALOG_MASTER_NAME = "CatalogMaster";
  public static final String CATALOG_MASTER_CLIENT_SERVICE_NAME = "CatalogMasterClient";
  public static final long CATALOG_MSTER_CLIENT_SERVICE_VERSION = 1;
}
