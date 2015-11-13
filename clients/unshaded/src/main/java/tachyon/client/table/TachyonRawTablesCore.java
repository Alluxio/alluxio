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
import tachyon.thrift.RawTableInfo;

@PublicApi
interface TachyonRawTablesCore {
  SimpleRawTable create(TachyonURI path, int numColumns, ByteBuffer metadata) throws IOException,
      TachyonException;

  RawTableInfo getInfo(SimpleRawTable rawTable) throws IOException, TachyonException;

  SimpleRawTable open(TachyonURI path) throws IOException, TachyonException;

  void updateRawTableMetadata(SimpleRawTable rawTable, ByteBuffer metadata) throws IOException,
      TachyonException;
}
