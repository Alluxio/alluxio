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

package tachyon.security.authentication;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;

import com.google.common.annotations.VisibleForTesting;

import tachyon.security.UserGroup;
import tachyon.thrift.MasterService;

public class TSetUserProcessor<T extends MasterService.Iface> extends MasterService
    .Processor<MasterService.Iface> {
  public TSetUserProcessor(MasterService.Iface iface) {
    super(iface);
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    setUserName(in);
    try {
      return super.process(in, out);
    } finally {
      UGI_TL.remove();
    }
  }

  private static final ThreadLocal<UserGroup> UGI_TL =
      new ThreadLocal<UserGroup>();

  public static UserGroup getRemoteUser() {
    return UGI_TL.get();
  }

  @VisibleForTesting
  public static void setRemoteUser(UserGroup ugi) {
    UGI_TL.set(ugi);
  }

  private void setUserName(final TProtocol in) {
    TTransport transport = in.getTransport();
    if (transport instanceof TSaslServerTransport) {
      String userName = ((TSaslServerTransport) transport).getSaslServer().getAuthorizationID();
      UGI_TL.set(UserGroup.createRemoteUser(userName));
    }
  }
}
