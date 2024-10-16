package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.io.InputStream;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        System.setProperty("hadoop.home.dir", "/");
        System.setProperty("HADOOP_USER_NAME", "edaniel");

        System.setProperty("socksProxyHost", "localhost");
        System.setProperty("socksProxyPort", "8080");

        Configuration conf = new HdfsConfiguration();
        conf.set("fs.defaultFS", "webhdfs://big-namenode1.vlandata.cls.fr:50070/");
        FileSystem fs = FileSystem.get(conf);

        System.out.println("File len : " + fs.getFileStatus(new Path("/user/edaniel/oui")).getLen());

        InputStream in = fs.open(new org.apache.hadoop.fs.Path("/user/edaniel/oui"));
        byte[] buffer = new byte[100];

        int readBytes = in.read(buffer);
        if (readBytes != -1) {
            System.out.println(new String(buffer, 0, readBytes));
        }

        in.close();
        java.nio.file.FileSystem fileSystem = java.nio.file.FileSystems.getDefault();

        HdfsFuse hdfsFuse = new HdfsFuse(fs);

        String[] fuseOpts = new String[] {
                "-o", "allow_other",
        };

        try {
            hdfsFuse.mount(fileSystem.getPath("/tmp/hdfs"), true, true, fuseOpts);
        } finally {
            hdfsFuse.umount();
        }
    }
}