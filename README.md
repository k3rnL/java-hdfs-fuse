# HDFS FUSE Native Image

This project is a FUSE filesystem that allows you to mount an HDFS filesystem on your local machine.

## Compatability
This project is built using GraalVM 21.2.0 and for now, it only supports Linux. 
I've never tried to compile on Windows or MacOS, so I can't guarantee that it will work. If you have any issues, please let me know.

## Usage
Download the latest binary release from the [releases page](https://github.com/k3rnL/java-hdfs-fuse/releases).
It is a standalone executable but you will need to have FUSE 3 installed on your system.
If you don't have FUSE installed, you can install it by running:
```bash
sudo apt-get install fuse3
```

To mount an HDFS filesystem, you can simply run the following command:
```bash
./hdfs-mount hdfs://<host>:<port> <mountPoint>
```

This is the full usage of the command:
```
Usage: hdfs-mount [-dh] [--proxy-host=<proxyHost>] [--proxy-port=<proxyPort>]
                  [-t=<target>] [-u=<user>] <server> <mountPoint>
                  [<fuseOptions>...]
Mount HDFS file system
      <server>             HDFS server URI, hdfs://<host>:<port> or webhdfs:
                             //<host>:<port>
      <mountPoint>         mounting point, local directory to mount the HDFS
                             file system, if it does not exist, it will be
                             created
      [<fuseOptions>...]   FUSE options. eg. -o allow_other,ro
  -d, --debug              enable fuse debug mode
  -h, --help               display a help message
      --proxy-host=<proxyHost>
                           SOCKS proxy host if needed for WebHDFS
      --proxy-port=<proxyPort>
                           SOCKS proxy port if needed for WebHDFS
  -t, --target=<target>    target directory in HDFS, default is /
  -u, --user=<user>        HDFS user name, default is the current user

```

## Building from source
To build the project from source, you need to have GraalVM 21.2.0 installed on your system.
You can build the project by running:
```bash
mvn package
```

This will generate a native image in the `target` directory.