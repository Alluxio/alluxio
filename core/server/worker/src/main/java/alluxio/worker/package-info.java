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

/**
 * Worker process and utils for working with the worker remotely.
 *
 * Main entry point for the worker is {@link alluxio.worker.AlluxioWorker#main}
 * which gets started by the alluxio start scripts. The {@link alluxio.worker.WorkerProcess}
 * spins up the different RPC services (thrift, data) which are mostly wrappers around
 * {@link alluxio.worker.block.BlockWorker}.
 *
 * <h1>Services</h1>
 *
 * <h2>DataServer</h2>
 *
 * This service is the main interaction between users and worker for reading and writing blocks.
 * The {@link alluxio.worker.DataServer} interface defines how to start/stop, and get port details;
 * to start, object init is used. The implementation of this interface is in
 * {@link alluxio.worker.netty.NettyDataServer}. It creates an {@link alluxio.worker.DataServer}
 * instance based on Netty which is an asynchronous event-driven network application framework.
 *
 * The current read protocol is described by {@link alluxio.proto.dataserver.Protocol.ReadRequest}
 * and the write protocol is described by {@link alluxio.proto.dataserver.Protocol.WriteRequest},
 * which are both defined by Protobuf to keep it extensible but also backward-compatible in the
 * future.
 *
 * <h2>Thrift</h2>
 *
 * The thrift service on worker side has been deprecated since v1.5. It used to serve
 * metadata operations (with a few operations that effect the worker's cached memory) such as lock
 * unlock, access, request space for blocks. It has been completely replaced by
 * the {@link alluxio.worker.DataServer} service.
 */
package alluxio.worker;
