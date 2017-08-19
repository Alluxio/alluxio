package alluxio.master.file;

import alluxio.thrift.FileSystemMasterClientService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Alluxio customized Thrift processor to handle RPC and track client IP.
 *
 * Original Thrift framework does not expose client IP because:
 * a) there is no such interface to query IP.
 * b) IP is not stored by Thrift.
 * c) The last stop from Thrift framework into Alluxio land is the
 *    {@link org.apache.thrift.ProcessFunction#process} method of
 *    {@link org.apache.thrift.ProcessFunction} class. This method has the information of in
 *    protocol from which we can derive IP. However, this method is made final deliberately.
 *    Consequently, it is very hard to subclass {@link org.apache.thrift.ProcessFunction}
 *    and override process method to pass IP informaton to Alluxio via arguments.
 * Based on a), b) and c), we decide to subclass the auto-generated
 * {@link FileSystemMasterClientService.Processor} class and
 * override its {@link org.apache.thrift.ProcessFunction#process} method. In this method, we store
 * the client IP as a thread-local variable for future use by {@link DefaultFileSystemMaster}.
 */
public class FileSystemMasterClientServiceProcessor extends FileSystemMasterClientService.Processor {
  private static ThreadLocal<String> mClientIpThreadLocal;

  public FileSystemMasterClientServiceProcessor(FileSystemMasterClientService.Iface handler) {
    super(handler);
    mClientIpThreadLocal = new ThreadLocal<>();
  }

  public static String getClientIp() { return mClientIpThreadLocal.get(); }

  @Override
  public boolean process(TProtocol in, TProtocol out) throws TException {
    TTransport transport = in.getTransport();
    if (transport instanceof TSaslServerTransport) {
      transport = ((TSaslServerTransport) transport).getUnderlyingTransport();
    }
    if (transport instanceof TSocket) {
      String ip = ((TSocket) transport).getSocket().getInetAddress().toString();
      mClientIpThreadLocal.set(ip);
    } else {
      mClientIpThreadLocal.set(null);
    }
    return super.process(in, out);
  }
}
