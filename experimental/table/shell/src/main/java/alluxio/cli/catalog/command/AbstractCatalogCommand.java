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

package alluxio.cli.catalog.command;

import alluxio.cli.Command;
import alluxio.client.catalog.CatalogMasterClient;
import alluxio.conf.AlluxioConfiguration;

/**
 * A class which should be extended when implementing commands for the
 * {@link alluxio.cli.catalog.CatalogShell}.
 */
public abstract class AbstractCatalogCommand implements Command {

  protected final AlluxioConfiguration mConf;
  protected final CatalogMasterClient mClient;

  /**
   * Creates a new instance of {@link AbstractCatalogCommand}.
   *
   * @param conf the alluxio configuration
   * @param client the client interface which can be used to make RPCs against the catalog master
   */
  public AbstractCatalogCommand(AlluxioConfiguration conf, CatalogMasterClient client) {
    mConf = conf;
    mClient = client;
  }

  @Override
  public abstract String getCommandName();
}
