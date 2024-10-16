package org.example;

import jnr.constants.platform.OpenFlags;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        System.setProperty("hadoop.home.dir", "/");
        System.setProperty("HADOOP_USER_NAME", "edaniel");
        System.setProperty("socksProxyHost", "localhost");
        System.setProperty("socksProxyPort", "8080");
//        System.setProperty("hadoop.socks.server", "localhost:8080");
//        System.setProperty("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.SocksSocketFactory");
        Configuration conf = new HdfsConfiguration();
        conf.set("fs.defaultFS", "webhdfs://big-namenode1.vlandata.cls.fr:50070/");
//        conf.set("hadoop.socks.server", "localhost:8080");
        conf.set("hadoop.rpc.socket.factory.class.default", "org.example.CustomSocketFactory");
//        conf.set("dfs.client.use.legacy.blockreader", "true");
//        conf.set("dfs.client.use.legacy.blockreader.local", "true");
//        conf.set("dfs.client.socket.timeout", "30000");  // Increase timeout if needed
//        conf.set("dfs.client.read.shortcircuit", "false");  // Disable short circuit reads
//        conf.set("dfs.client.read.prefetch.size", "1048576"); // Set prefetch size for reads
//        System.setProperty("dfs.client.use.legacy.blockreader", "true");

        FileSystem fs = FileSystem.get(conf);
//        var dirs = fs.listLocatedStatus(new Path("/prod"));
//
//        System.out.println(fs.getFileStatus(new Path("/prod/mar/data")).getOwner());
//        System.out.println(fs.getWorkingDirectory().toString());
//
//        System.out.println(dirs.hasNext());
//        while (dirs.hasNext()) {
//            var dir = dirs.next();
//            System.out.println(dir.getOwner());
//            System.out.println(dir.toString());
//        }
//
//
//
//        System.out.println("Done");

//        var handle = fs.create(new org.apache.hadoop.fs.Path("/user/edaniel/test"), true);
//        handle.writeBytes("Hello World");
//        handle.close();

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

        try {
            hdfsFuse.mount(fileSystem.getPath("/tmp/hdfs"), true, true);
        } finally {
            hdfsFuse.umount();
        }
    }
}