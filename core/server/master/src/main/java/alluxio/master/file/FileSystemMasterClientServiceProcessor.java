package alluxio.master.file;

import alluxio.thrift.FileSystemMasterClientService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class FileSystemMasterClientServiceProcessor extends FileSystemMasterClientService.Processor {
  private static ThreadLocal<String> mClientIpThreadLocal;

  public FileSystemMasterClientServiceProcessor(FileSystemMasterClientService.Iface handler) {
    super(handler);
    mClientIpThreadLocal = new ThreadLocal<>();
  }

  public static String getClientIpThreadLocal() { return mClientIpThreadLocal.get(); }

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
