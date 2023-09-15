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

package alluxio.conf;

import java.util.List;
import java.util.Map;

public class UpdatedConfigEventDiff {
  List<Map<String, String>> mChangedProperties;
  long mVersion;

  public UpdatedConfigEventDiff(List<Map<String, String>> changedProperties, long version) {
    mChangedProperties = changedProperties;
    mVersion = version;
  }

  public List<Map<String, String>> getChangedProperties() {
    return mChangedProperties;
  }

  public void setChangedProperties(List<Map<String, String>> changedProperties) {
    mChangedProperties = changedProperties;
  }

  public long getVersion() {
    return mVersion;
  }

  public void setVersion(long version) {
    mVersion = version;
  }
}
