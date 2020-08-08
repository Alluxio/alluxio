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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class LsPathInAlluxio extends AbstractFileSystemCommand {

    private static final String READABLE_OPTION_NAME = "h";

    private static final Option READABLE_OPTION =
            Option.builder(READABLE_OPTION_NAME)
                    .required(false)
                    .hasArg(false)
                    .desc("print sizes in human readable format (e.g., 1KB 234MB 2GB)")
                    .build();

    public LsPathInAlluxio(@Nullable FileSystemContext fsContext) {
        super(fsContext);
    }

    /**
     * Displays information for In-Alluxio data size under the path group by worker.
     *
     * @param path The {@link AlluxioURI} path as the input of the command
     * @param readable whether to print info of human readable format
     */
    private void lsPathInAlluxio(AlluxioURI path, boolean readable) throws AlluxioException, IOException {

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Getting directory status of %s files or sub-directories "
                        + "may take a while.");
            }
        }, 10000);
        ListStatusPOptions.Builder optionsBuilder = ListStatusPOptions.newBuilder();
        optionsBuilder.setRecursive(true);
        List<URIStatus> statuses = mFileSystem.listStatus(path, optionsBuilder.build());
        timer.cancel();

        Map<String, Long> distributionMap = new HashMap<>();
        for (URIStatus status: statuses) {
            for (FileBlockInfo fileBlockInfo : status.getFileBlockInfos()) {
                Long length = fileBlockInfo.getBlockInfo().getLength();
                for (BlockLocation blockLocation : fileBlockInfo.getBlockInfo().getLocations()) {
                    distributionMap.put(blockLocation.getWorkerAddress().getHost(),
                        distributionMap.getOrDefault(blockLocation.getWorkerAddress().getHost(), 0L) + length);
                }
            }
        }

        distributionMap.forEach((workerHostName, size) -> {
            printInfo(workerHostName, readable ? FormatUtils.getSizeFromBytes(size) : String.valueOf(size));
        });
    }

    /**
     * Prints the size messages.
     *
     * @param workerHostName Host name of the worker
     * @param inAlluxioMessage the in Alluxio size message to print
     */
    private static void printInfo(String workerHostName, String inAlluxioMessage) {
        System.out.println(String.format("%-25s %s", workerHostName, inAlluxioMessage));
    }

    @Override
    protected void processHeader(CommandLine cl) {
        printInfo("Worker Host Name", "In Alluxio");
    }

    @Override
    protected void runPlainPath(AlluxioURI path, CommandLine cl)
            throws AlluxioException, IOException {
        lsPathInAlluxio(path, cl.hasOption(READABLE_OPTION_NAME));
    }

    @Override
    public int run(CommandLine cl) throws AlluxioException, IOException {
        String[] args = cl.getArgs();
        AlluxioURI path = new AlluxioURI(args[0]);
        runWildCardCmd(path, cl);
        return 0;
    }

    @Override
    public String getCommandName() {
        return "lsPathInAlluxio";
    }

    @Override
    public String getUsage() {
        return "lsPathInAlluxio [-h] <path>";
    }

    @Override
    public String getDescription() {
        return "Displays information for In-Alluxio data size under the path group by worker.";
    }

    @Override
    public Options getOptions() {
        return new Options().addOption(READABLE_OPTION);
    }
}
