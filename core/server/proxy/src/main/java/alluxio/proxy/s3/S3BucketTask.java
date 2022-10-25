package alluxio.proxy.s3;

public class S3BucketTask implements S3BaseTask {

    public S3BucketTask() {}

    public static S3BucketTask createTask(String bucket, S3BaseHandler handler) {
        return new S3BucketTask();
    }
    @Override
    public void handleTask() {

    }
}
