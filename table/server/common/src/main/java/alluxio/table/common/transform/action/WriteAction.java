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

package alluxio.table.common.transform.action;

import java.util.List;
import java.util.Map;

/**
 * The definition of the write action.
 */
public class WriteAction implements TransformAction {
  private static final String NAME = "write";

  /**
   * Factory to create an instance.
   */
  public static class WriteActionFactory implements TransformActionFactory {
    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public TransformAction create(String definition, List<String> args,
        Map<String, String> options) {
      return new WriteAction();
    }
  }

  private WriteAction() {
    // TODO(cc): implement
  }
}
