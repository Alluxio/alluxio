package alluxio.master.block;

import alluxio.grpc.GetRegisterLeasePRequest;

public interface RegisterLeaseReviewer {
  boolean reviewLeaseRequest(GetRegisterLeasePRequest request);
}
