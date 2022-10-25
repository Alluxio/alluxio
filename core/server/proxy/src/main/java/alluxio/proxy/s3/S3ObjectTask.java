package alluxio.proxy.s3;

public class S3ObjectTask implements S3BaseTask{

    @Override
    public void handleTask() {

    }

    public static S3ObjectTask createTask(String bucket, String object, S3BaseHandler handler) {
        return new S3ObjectTask();
    }

}
