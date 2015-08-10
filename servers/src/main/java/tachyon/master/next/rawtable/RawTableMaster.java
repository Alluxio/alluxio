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

package tachyon.master.next.rawtable;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.next.Master;

// TODO: implement this master, but should be deprecated.
public class RawTableMaster implements Master {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public RawTableMaster() {
  }

  @Override
  public TProcessor getProcessor() {
    return null;
  }

  @Override
  public String getProcessorName() {
    return "RawTableMaster";
  }

}
