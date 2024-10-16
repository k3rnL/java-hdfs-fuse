package com.k3rnl.hdfs.fuse;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, ParseException {
        Options options = new Options();
        options.addOption("h", "help", false, "print this message");
        options.addOption("o", "options", true, "FUSE options");
        options.addOption("s", "server", true, "HDFS server URI, hdfs://<host>:<port> or webhdfs://<host>:<port>");
        options.addOption("u", "user", true, "HDFS user name");

        CommandLineParser cmd = new PosixParser();
        CommandLine cl = cmd.parse(options, args);

        System.setProperty("hadoop.home.dir", "/");
        System.setProperty("HADOOP_USER_NAME", cl.getOptionValue("o"));

        System.setProperty("socksProxyHost", "localhost");
        System.setProperty("socksProxyPort", "8080");

        Configuration conf = new HdfsConfiguration();
        conf.set("fs.defaultFS", cl.getOptionValue("s"));
        FileSystem fs = FileSystem.get(conf);

        java.nio.file.FileSystem fileSystem = java.nio.file.FileSystems.getDefault();

        HdfsFuse hdfsFuse = new HdfsFuse(fs);

        List<String> opts = new ArrayList<>();
        for (var opt : cl.getOptionValues("o")) {
            opts.add("-o");
            opts.add(opt);
        }
        String[] fuseOpts = opts.toArray(new String[0]);

        try {
            hdfsFuse.mount(fileSystem.getPath("/tmp/hdfs"), true, true, fuseOpts);
        } finally {
            hdfsFuse.umount();
        }
    }
}