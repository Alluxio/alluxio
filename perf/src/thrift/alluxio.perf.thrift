namespace java alluxio.perf.thrift

// Version 1: 0.1.0 (Thrift version is 0.9.1 which is in keeping with Alluxio-0.6.0)

exception SlaveAlreadyRegisterException {
  1: string message
}

exception SlaveNotRegisterException {
  1: string message
}

service MasterService {
  bool slave_canRun(1: i32 taskId, 2: string nodeName)
    throws (1: SlaveNotRegisterException e)

  void slave_finish(1: i32 taskId, 2: string nodeName, 3: bool successFinish)
    throws (1: SlaveNotRegisterException e)

  void slave_ready(1: i32 taskId, 2: string nodeName, 3: bool successSetup)
    throws (1: SlaveNotRegisterException e)

  bool slave_register(1: i32 taskId, 2: string nodeName, 3: string cleanupDir)
    throws (1: SlaveAlreadyRegisterException e)
}
