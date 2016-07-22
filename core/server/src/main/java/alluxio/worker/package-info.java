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
 * Main entry point for the worker is {@link alluxio.worker.AlluxioWorker#main(String[])}
 * which gets started by the alluxio start scripts. The {@link alluxio.worker.AlluxioWorkerService}
 * spins up the different RPC services (thrift, data) which are mostly wrappers around
 * {@link alluxio.worker.block.BlockWorker}.
 *
 * <h1>Services</h1>
 *
 * <h2>Thrift</h2>
 *
 * The thrift service is mostly responsible for metadata operations (with a few operations that
 * effect the worker's cached memory).
 *
 * <h3>Cache Block</h3>
 *
 * Move's user generated blocks to the alluxio data directory. This operation expects that the
 * caller is a local (to the node) caller, and that the input are under the user directories.
 *
 * Implementation can be found at {@link alluxio.worker.block.BlockWorker#commitBlock(long, long)}
 *
 * <h3>Lock / Unlock</h3>
 *
 * Alluxio supports caching blocks to local disk (client side). When this happens, a lock is given
 * to the client to protect it from the remote block changing.
 *
 * Implementation can be found at {@link alluxio.worker.block.BlockWorker#lockBlock(long, long)} and
 * {@link alluxio.worker.block.BlockWorker#unlockBlock(long, long)}.
 *
 * <h2>Data</h2>
 *
 * This service is the main interaction between users and reading blocks. Currently this service
 * only supports reading blocks (writing is to local disk).
 *
 * There are two different implementations of this layer:
 * {@link alluxio.worker.netty.NettyDataServer} and {@link alluxio.worker.nio.NIODataServer}; netty
 * is the default. To support both, a {@link alluxio.worker.DataServer} interface is used that just
 * defines how to start/stop, and get port details; to start, object init is used.
 *
 * The current read protocol is defined in {@link alluxio.worker.DataServerMessage}. This has two
 * different types: read {@link alluxio.worker.DataServerMessage#createBlockRequestMessage} and
 * write {@link alluxio.worker.DataServerMessage#createBlockResponseMessage}. Side note, the netty
 * implementation does not use this class, but has defined two classes for the read and write case:
 * {@link alluxio.network.protocol.RPCBlockReadRequest},
 * {@link alluxio.network.protocol.RPCBlockReadResponse}; theses classes are network compatible.
 */

package alluxio.worker;
