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

package alluxio.worker.netty;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class NettyReadHandlerStateMachineTest {

  @Test
  public void testGenerateStateDiagram() throws IOException  {
    EmbeddedChannel embeddedChannel = new EmbeddedChannel();
    NettyReadHandlerStateMachine<ReadRequest> stateMachine =
        new NettyReadHandlerStateMachine<>(
            embeddedChannel, ReadRequest.class, (readRequest) -> null);
    stateMachine.generateStateDiagram(new File("output.dot").toPath());
  }
}
