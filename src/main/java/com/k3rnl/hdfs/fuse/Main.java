package com.k3rnl.hdfs.fuse;

import com.k3rnl.fuse.FuseNative;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "hdfs-mount", description = "Mount HDFS file system")
public class Main implements Callable<Integer> {

    @Parameters(index = "0", description = "HDFS server URI, hdfs://<host>:<port> or webhdfs://<host>:<port>")
    private String server;

    @Parameters(index = "1", description = "mounting point, local directory to mount the HDFS file system, if it does not exist, it will be created")
    private String mountPoint;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested;

    @Option(names = {"-u", "--user"},  description = "HDFS user name, default is the current user")
    private String user;

    @Option(names = {"-t", "--target"}, description = "target directory in HDFS, default is /", defaultValue = "/")
    private String target;

    @Option(names = {"--proxy-host"}, description = "SOCKS proxy host if needed for WebHDFS")
    private String proxyHost;

    @Option(names = {"--proxy-port"}, description = "SOCKS proxy port if needed for WebHDFS")
    private int proxyPort;

    @Option(names = {"-d", "--debug"}, description = "enable fuse debug mode")
    private boolean debug;

    @Parameters(index = "2..*", description = "FUSE options. eg. -o allow_other,ro")
    private final List<String> fuseOptions = new ArrayList<>();

    @Override
    public Integer call() throws Exception {

        System.setProperty("hadoop.home.dir", target);

        if (user != null) {
            System.setProperty("HADOOP_USER_NAME", user);
        }

        if (proxyHost != null) {
            System.setProperty("socksProxyHost", proxyHost);
            if (proxyPort > 0) {
                System.setProperty("socksProxyPort", Integer.toString(proxyPort));
            }
        }

        var mountPointFile = new java.io.File(mountPoint);
        if (!mountPointFile.exists()) {
            if (!mountPointFile.mkdirs()) {
                System.err.println("Failed to create mounting point: " + mountPoint);
                return 1;
            }
        }

        Configuration conf = new HdfsConfiguration();
        conf.set("fs.defaultFS", server);

        try {
            var fs = FileSystem.get(conf);

            HdfsFuseOperations fuseOps = new HdfsFuseOperations(fs);
            FuseNative fuse = new FuseNative(fuseOps);
            fuse.mount(mountPoint, debug, fuseOptions);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main())
                .setStopAtPositional(true)
                .execute(args);
        System.exit(exitCode);
    }

}