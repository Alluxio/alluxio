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

package alluxio.table.common;

/**
 * The layout factory interface.
 */
public interface LayoutFactory {

  /**
   * @return the type of layout for the factory
   */
  String getType();

  /**
   * @param layoutProto the proto representation of the layout
   * @return a new instance of the layout
   */
  Layout create(alluxio.grpc.table.Layout layoutProto);
}
