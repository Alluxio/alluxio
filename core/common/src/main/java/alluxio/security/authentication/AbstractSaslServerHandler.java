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

package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;
import alluxio.grpc.SaslMessageType;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Abstract {@link SaslServerHandler} implementation that maintains {@link SaslServer} instance.
 */
public abstract class AbstractSaslServerHandler implements SaslServerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSaslServerHandler.class);

  /** Underlying {@code SaslServer}. */
  protected SaslServer mSaslServer;

  @Override
  public SaslMessage handleMessage(SaslMessage message) throws SaslException {
    switch (message.getMessageType()) {
      case CHALLENGE:
        byte[] saslResponse = mSaslServer.evaluateResponse(message.getMessage().toByteArray());
        SaslMessage.Builder response = SaslMessage.newBuilder();
        if (mSaslServer.isComplete()) {
          response.setMessageType(SaslMessageType.SUCCESS);
        } else {
          response.setMessageType(SaslMessageType.CHALLENGE);
        }
        if (saslResponse != null) {
          response.setMessage(ByteString.copyFrom(saslResponse));
        }
        return response.build();
      default:
        throw new SaslException(
            "Server can't process Sasl message type:" + message.getMessageType().name());
    }
  }

  @Override
  public void close() {
    if (mSaslServer != null) {
      try {
        mSaslServer.dispose();
      } catch (SaslException exc) {
        LOG.debug("Failed to close SaslServer.", exc);
      }
    }
  }
}
