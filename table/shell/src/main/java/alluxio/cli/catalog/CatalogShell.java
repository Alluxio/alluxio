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

package alluxio.cli.catalog;

import alluxio.ClientContext;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.client.catalog.CatalogMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.util.ConfigurationUtils;

import java.util.Map;

/**
 * A shell implementation which is used to load the commands for interacting with the Alluxio
 * catalog service.
 */
public class CatalogShell extends AbstractShell {
  private static final String SHELL_NAME = "catalog";

  /**
   * Construct a new instance of {@link CatalogShell}.
   *
   * @param conf the Alluxio configuration to use when instantiating the shell
   */
  public CatalogShell(InstancedConfiguration conf) {
    super(null, null, conf);
  }

  /**
   * Manage Alluxio extensions.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    CatalogShell catalogShell =
        new CatalogShell(new InstancedConfiguration(ConfigurationUtils.defaults()));
    System.exit(catalogShell.run(args));
  }

  @Override
  public Map<String, Command> loadCommands() {
    return CommandUtils.loadCommands(CatalogShell.class.getPackage().getName(),
        new Class[] {AlluxioConfiguration.class, CatalogMasterClient.class},
        new Object[] {mConfiguration, mCloser.register(CatalogMasterClient.Factory.create(
            MasterClientContext.newBuilder(ClientContext.create(mConfiguration)).build()))}
        );
  }

  @Override
  public String getShellName() {
    return SHELL_NAME;
  }
}
