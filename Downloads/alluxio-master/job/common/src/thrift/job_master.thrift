namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

enum Status {
  UNKNOWN = 0,
  CREATED = 1,
  CANCELED = 2,
  FAILED = 3,
  RUNNING = 4,
  COMPLETED = 5,
}

struct TaskInfo {
  1: i64 jobId
  2: i32 taskId
  3: Status status
  4: string errorMessage
  5: binary result
}

struct JobInfo {
  1: i64 id
  2: string errorMessage
  3: list<TaskInfo> taskInfos
  4: Status status
  5: string result
}

union JobCommand {
  1: optional RunTaskCommand runTaskCommand
  2: optional CancelTaskCommand cancelTaskCommand
  3: optional RegisterCommand registerCommand
}

struct CancelTaskCommand {
  1: i64 jobId
  2: i32 taskId
}

struct RegisterCommand {}

struct RunTaskCommand {
  1: i64 jobId
  2: i32 taskId
  3: binary jobConfig
  4: binary taskArgs
}

struct CancelTOptions {}
struct CancelTResponse {}

struct GetJobStatusTOptions {}
struct GetJobStatusTResponse {
  1: JobInfo jobInfo
}

struct ListAllTOptions {}
struct ListAllTResponse {
  1: list<i64> jobIdList
}

struct RunTOptions {}
struct RunTResponse {
  1: i64 jobId
}

/**
 * This interface contains job master service endpoints for job service clients.
 */
service JobMasterClientService extends common.AlluxioService {

  /**
   * Cancels the given job.
   */
  CancelTResponse cancel(
    /** the job id */ 1: i64 id,
    /** the method options */ 2: CancelTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Gets the status of the given job.
   */
  GetJobStatusTResponse getJobStatus(
    /** the job id */ 1: i64 id,
    /** the method options */ 2: GetJobStatusTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Lists ids of all known jobs.
   */
  ListAllTResponse listAll(
    /** the method options */ 1: ListAllTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Starts the given job, returning a job id.
   */
  RunTResponse run(
    /** the command line job info */ 1: binary jobConfig,
    /** the method options */ 2: RunTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}

struct JobHeartbeatTOptions {}
struct JobHeartbeatTResponse {
  1: list<JobCommand> commands
}

struct RegisterJobWorkerTOptions {}
struct RegisterJobWorkerTResponse {
 1: i64 id
}

/**
 * This interface contains job master service endpoints for job service workers.
 */
service JobMasterWorkerService extends common.AlluxioService {

  /**
   * Periodic worker heartbeat returns a list of commands for the worker to execute.
   */
  JobHeartbeatTResponse heartbeat(
    /** the id of the worker */ 1: i64 workerId,
    /** the list of tasks status */ 2: list<TaskInfo> taskInfoList,
    /** the method options */ 3: JobHeartbeatTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns a worker id for the given network address.
   */
  RegisterJobWorkerTResponse registerJobWorker(
    /** the worker network address */ 1: common.WorkerNetAddress workerNetAddress,
    /** the method options */ 2: RegisterJobWorkerTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
