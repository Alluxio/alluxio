package alluxio.master;

import alluxio.grpc.AlluxioSaslClientServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.security.authentication.AuthenticatedClientRegistry;
import alluxio.security.authentication.SaslStreamServerDriver;
import io.grpc.stub.StreamObserver;

public class AlluxioSaslClientServiceHandler
    extends AlluxioSaslClientServiceGrpc.AlluxioSaslClientServiceImplBase {

  private AuthenticatedClientRegistry mClientRegistry = null;

  public AlluxioSaslClientServiceHandler(AuthenticatedClientRegistry clientRegistry) {
    mClientRegistry = clientRegistry;
  }

  @Override
  public StreamObserver<SaslMessage> authenticate(StreamObserver<SaslMessage> responseObserver) {
    SaslStreamServerDriver driver = new SaslStreamServerDriver(mClientRegistry);
    driver.setClientObserver(responseObserver);
    return driver;
  }
}
