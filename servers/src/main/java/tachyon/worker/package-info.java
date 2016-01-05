/**
 * Worker process and utils for working with the worker remotely.
 *
 * Main entry point for the worker is {@link tachyon.worker.TachyonWorker#main(String[])} which gets
 * started by the tachyon start scripts. The {@link tachyon.worker.TachyonWorker} class spins up the
 * different RPC services (thrift, data) which are mostly wrappers around
 * {@link tachyon.worker.block.BlockDataManager}.
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
 * Move's user generated blocks to the tachyon data directory. This operation expects that the
 * caller is a local (to the node) caller, and that the input are under the user directories.
 *
 * Implementation can be found at
 * {@link tachyon.worker.block.BlockDataManager#commitBlock(long, long)}
 *
 * <h3>Lock / Unlock</h3>
 *
 * Tachyon supports caching blocks to local disk (client side). When this happens, a lock is given
 * to the client to protect it from the remote block changing.
 *
 * Implementation can be found at
 * {@link tachyon.worker.block.BlockDataManager#lockBlock(long, long)} and
 * {@link tachyon.worker.block.BlockDataManager#unlockBlock(long, long)}.
 *
 * <h2>Data</h2>
 *
 * This service is the main interaction between users and reading blocks. Currently this service
 * only supports reading blocks (writing is to local disk).
 *
 * There are two different implementations of this layer:
 * {@link tachyon.worker.netty.NettyDataServer} and {@link tachyon.worker.nio.NIODataServer}; netty
 * is the default. To support both, a {@link tachyon.worker.DataServer} interface is used that just
 * defines how to start/stop, and get port details; to start, object init is used.
 *
 * The current read protocol is defined in {@link tachyon.worker.DataServerMessage}. This has two
 * different types: read {@link tachyon.worker.DataServerMessage#createBlockRequestMessage} and
 * write {@link tachyon.worker.DataServerMessage#createBlockResponseMessage}. Side note, the netty
 * implementation does not use this class, but has defined two classes for the read and write case:
 * {@link tachyon.network.protocol.RPCBlockReadRequest},
 * {@link tachyon.network.protocol.RPCBlockReadResponse}; theses classes are network compatible.
 */
package tachyon.worker;
