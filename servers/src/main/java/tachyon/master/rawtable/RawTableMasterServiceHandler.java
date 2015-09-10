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

package tachyon.master.rawtable;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import tachyon.TachyonURI;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.RawTableInfo;
import tachyon.thrift.RawTableMasterService;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;
import tachyon.thrift.TachyonException;

public class RawTableMasterServiceHandler implements RawTableMasterService.Iface {
  private final RawTableMaster mRawTableMaster;

  public RawTableMasterServiceHandler(RawTableMaster rawTableMaster) {
    mRawTableMaster = rawTableMaster;
  }

  @Override
  public long createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException,
      TachyonException, TException {
    return mRawTableMaster.createRawTable(new TachyonURI(path), columns, metadata);
  }

  @Override
  public long getRawTableId(String path)
      throws InvalidPathException, TableDoesNotExistException, TException {
    return mRawTableMaster.getRawTableId(new TachyonURI(path));
  }

  @Override
  public RawTableInfo getClientRawTableInfoById(long id)
      throws TableDoesNotExistException, TException {
    return mRawTableMaster.getClientRawTableInfo(id);
  }

  @Override
  public RawTableInfo getClientRawTableInfoByPath(String path)
      throws TableDoesNotExistException, InvalidPathException, TException {
    return mRawTableMaster.getClientRawTableInfo(new TachyonURI(path));
  }

  @Override
  public void updateRawTableMetadata(long tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TachyonException, TException {
    mRawTableMaster.updateRawTableMetadata(tableId, metadata);
  }
}
