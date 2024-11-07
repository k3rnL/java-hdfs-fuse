package com.k3rnl.hdfs.fuse;

import com.k3rnl.fuse.api.FillDir;
import com.k3rnl.fuse.api.JavaFuseOperations;
import com.k3rnl.fuse.fuse.*;
import com.k3rnl.fuse.libc.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.graalvm.nativeimage.StackValue;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class HdfsFuseOperations extends JavaFuseOperations {

    private final Map<Long, FileReadInfo> openFiles = new ConcurrentHashMap<>();
    private final Map<Long, FileWriteInfo> openWriteFiles = new ConcurrentHashMap<>();
    private final AtomicLong handleCounter = new AtomicLong();

    protected FileSystem fs;

    public HdfsFuseOperations(FileSystem fs) {
        this.fs = fs;
    }

    public record FileReadInfo(SeekableBufferedInputStream in, Path path) {}

    public static class FileWriteInfo {
        final FSDataOutputStream out;
        final Path path;
        long lastOffset;

        public FileWriteInfo(FSDataOutputStream out, Path path) {
            this.out = out;
            this.path = path;
        }
    }

    private static void fileStat(FileStatus status, FileStat stat) {
        var permission = status.getPermission().toShort();
        stat.st_gid(1000);
        stat.st_uid(1000);
        if (status.isFile())
            stat.st_mode(FileStatFlags.S_IFREG | permission);
        else if (status.isDirectory())
            stat.st_mode(FileStatFlags.S_IFDIR | permission);
        stat.st_size(status.getLen());
        stat.st_nlink(1);
        stat.st_atime().tv_sec(status.getAccessTime() / 1000);
        stat.st_mtime().tv_sec(status.getModificationTime() / 1000);
        stat.st_ctime().tv_sec(status.getModificationTime() / 1000);
    }

    @Override
    public int getattr(String path, FileStat stat, FuseFileInfo fi) {
        try {
            var status = fs.getFileStatus(new Path(path));
            fileStat(status, stat);
            return 0;
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (AccessControlException e) {
            return -Errno.EACCES();
        } catch (IOException e) {
            System.err.println("Error getting file status for path: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }

    @Override
    public int readdir(String path, VoidPointer buf, FillDir filter, long offset, FuseFileInfo fi, FuseReaddirFlags flags) {
        try {
            var folderStatus = fs.getFileStatus(new Path(path));
            if (!folderStatus.isDirectory()) {
                return -Errno.ENOTDIR();
            }
            FileStat stat = StackValue.get(FileStat.class);
            var status = fs.listStatus(new Path(path));
            fileStat(folderStatus, stat);
            filter.apply(buf, ".", stat, 0, FuseFillDirFlags.FUSE_FILL_DIR_PLUS);
            filter.apply(buf, "..", WordFactory.nullPointer(), 0, FuseFillDirFlags.FUSE_FILL_DIR_PLUS);
            for (var fileStatus : status) {
                fileStat(fileStatus, stat);
                filter.apply(buf, fileStatus.getPath().getName(), stat, 0, FuseFillDirFlags.FUSE_FILL_DIR_PLUS);
            }
            return 0;
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error listing directory: " + path);
            return -Errno.EIO();
        }
    }

    @Override
    public int create(String path, long mode, FuseFileInfo fi) {
        Path filePath = new Path(path);
        try {
            FsPermission permission = new FsPermission((short) (mode & 0777));
            // Create and open the file for writing
            fs.create(filePath, permission, true, 4096, fs.getDefaultReplication(filePath), fs.getDefaultBlockSize(filePath), null)
                    .close(); // force the file to exists
            FSDataOutputStream out = fs.append(filePath);
            long handle = handleCounter.incrementAndGet();
            FileWriteInfo writeInfo = new FileWriteInfo(out, filePath);
            writeInfo.lastOffset = 0;
            openWriteFiles.put(handle, writeInfo);
            fi.fh(handle); // Set the file handle
            return 0;
        } catch (IOException e) {
            System.err.println("Error creating file: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }


    @Override
    public int open(String path, FuseFileInfo fi) {
        Path filePath = new Path(path);
        int flags = fi.flags();
        int accessMode = flags & OpenFlags.O_ACCMODE;

        try {
            FileStatus status = null;
            boolean fileExists = fs.exists(filePath);
            if (fileExists) {
                status = fs.getFileStatus(filePath);
                if (status.isDirectory()) {
                    return -Errno.EISDIR();
                }
            }

            // Handle creation flags
            if ((flags & OpenFlags.O_CREAT) != 0) {
                if (!fileExists) {
                    // Create the file
                    fs.create(filePath).close();
                    fileExists = true;
                } else {
                    if ((flags & OpenFlags.O_EXCL) != 0) {
                        return -Errno.EEXIST();
                    }
                }
            } else {
                if (!fileExists) {
                    return -Errno.ENOENT();
                }
            }

//            // Handle truncation
//            boolean truncate = (flags & OpenFlags.O_TRUNC) != 0;

            // Assign a unique handle
            long handle = handleCounter.incrementAndGet();

            if (accessMode == OpenFlags.O_RDONLY) {
                // Open for reading
                SeekableBufferedInputStream in = new SeekableBufferedInputStream(fs.open(filePath), 2048 * 1024, 20);
                openFiles.put(handle, new FileReadInfo(in, filePath));
                fi.fh(handle);
            } else if (accessMode == OpenFlags.O_WRONLY || accessMode == OpenFlags.O_RDWR) {
                // Open for writing or reading and writing
                FileWriteInfo writeInfo;
                FSDataOutputStream out;

                if ((flags & OpenFlags.O_APPEND) != 0) {
                    // Open for appending
                    out = fs.append(filePath);
                    writeInfo = new FileWriteInfo(out, filePath);
                    writeInfo.lastOffset = (int) status.getLen();
                } else {
                    // Open for writing
                    out = fs.create(filePath, true);
                    writeInfo = new FileWriteInfo(out, filePath);
//                    writeInfo.lastOffset = truncate ? 0 : (int) status.getLen();
                }

                // If access mode is O_RDWR, also handle reading
                if (accessMode == OpenFlags.O_RDWR) {
                    SeekableBufferedInputStream in = new SeekableBufferedInputStream(fs.open(filePath), 2048 * 1024, 20);
                    openFiles.put(handle, new FileReadInfo(in, filePath));
                }

                openWriteFiles.put(handle, writeInfo);
                fi.fh(handle);
            } else {
                // Unsupported access mode
                return -Errno.EACCES();
            }

            return 0;
        } catch (AccessControlException e) {
            return -Errno.EACCES();
        } catch (IOException e) {
            System.err.println("Error opening file: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }


    @Override
    public int release(String path, FuseFileInfo fi) {
        long handle = fi.fh();

        // Close input stream if it's open
        FileReadInfo in = openFiles.remove(handle);
        if (in != null) {
            try {
                in.in.close();
            } catch (IOException e) {
                System.err.println("Error closing input stream for file: " + path);
                e.printStackTrace();
                return -Errno.EIO();
            }
        }

        // Close output stream if it's open
        FileWriteInfo writeInfo = openWriteFiles.remove(handle);
        if (writeInfo != null) {
            try {
                writeInfo.out.close();
            } catch (IOException e) {
                System.err.println("Error closing output stream for file: " + path);
                e.printStackTrace();
                return -Errno.EIO();
            }
        }

        return 0;
    }

    @Override
    public int rmdir(String path) {
        try {
            var status = fs.getFileStatus(new Path(path));
            if (!status.isDirectory()) {
                return -Errno.ENOTDIR();
            }
            fs.delete(new Path(path), true);
            return 0;
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error deleting directory: " + path);
            return -Errno.EIO();
        }
    }

    @Override
    public int unlink(String path) {
        try {
            var status = fs.getFileStatus(new Path(path));
            if (status.isDirectory()) {
                return -Errno.EISDIR();
            }
            fs.delete(new Path(path), false);
            return 0;
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error deleting file: " + path);
            return -Errno.EIO();
        }
    }

    @Override
    public int read(String path, byte[] buf, long size, long offset, FuseFileInfo fi) {
        long handle = fi.fh();
        FileReadInfo info = openFiles.get(handle);
        SeekableBufferedInputStream in = info.in;

        if (in == null) {
            return -Errno.EBADF(); // Invalid file handle
        }

        try {
            synchronized (in) {
                in.seek(offset);
                byte[] data = new byte[(int) size];
                int bytesRead = 0;
                int totalBytesRead = 0;
                int bytesToRead = (int) size;

                totalBytesRead = in.readNBytes(data, totalBytesRead, bytesToRead);

                if (totalBytesRead > 0) {
                    System.arraycopy(data, 0, buf, 0, totalBytesRead);
                    return totalBytesRead;
                } else {
                    return 0; // EOF
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading from file: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        } catch (Exception e) {
            e.printStackTrace();
            return -Errno.EIO();
        }
    }

    @Override
    public int write(String path, byte[] buf, long size, long offset, FuseFileInfo fi) {
        long handle = fi.fh();
        FileWriteInfo writeInfo = openWriteFiles.get(handle);

        if (writeInfo == null) {
            System.out.println("Invalid file handle: " + handle);
            System.out.println("Open files: " + openWriteFiles);
            return -Errno.EBADF(); // Invalid file handle
        }

        try {
            if (offset < writeInfo.lastOffset) {
                // Writing before the last written offset is not supported
                return -Errno.EINVAL();
            }

            if (offset > writeInfo.lastOffset) {
                // Need to fill the gap between lastOffset and offset with zeros
                long gapSize = offset - writeInfo.lastOffset;
                byte[] zeros = new byte[(int) gapSize];
                writeInfo.out.write(zeros);
                writeInfo.lastOffset += gapSize;
            }

            byte[] data = new byte[(int) size];
            for (long i = 0; i < size; i++) {
                data[(int) i] = buf[(int) i];
            }
            writeInfo.out.write(data);
            writeInfo.lastOffset += size;

            return (int) size;
        } catch (IOException e) {
            System.err.println("Error writing to file: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }

    @Override
    public int utimens(String path, TimeSpec[] timespec, FuseFileInfo fi) {
        try {
            var status = fs.getFileStatus(new Path(path));
            fs.setTimes(new Path(path), timespec[0].tv_sec(), timespec[1].tv_sec());
            return 0;
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error setting file times: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }
    @Override
    public int statfs(String path, StatVFS stat) {
        try {
            final var blockSize = fs.getDefaultBlockSize(new Path("/"));
            var status = fs.getStatus();
            stat.f_blocks(status.getCapacity() / blockSize);
            stat.f_bfree(status.getRemaining() / blockSize);
            stat.f_bavail(status.getRemaining() / blockSize);
            stat.f_bsize(blockSize);
            stat.f_frsize(blockSize);
            return 0;
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error getting file status for path: " + path);
            return -Errno.EIO();
        }
    }

    @Override
    public int mkdir(String path, int mode) {
        try {
            FsPermission permission = new FsPermission((short) (mode & 0777));
            fs.mkdirs(new Path(path), permission);
            return 0;
        } catch (IOException e) {
            System.err.println("Error creating directory: " + path);
            return -Errno.EIO();
        }
    }

    @Override
    public int mknod(String path, int mode, int rdev) {
        try {
            FsPermission permission = new FsPermission((short) (mode & 0777));
            fs.create(new Path(path), permission, true, 4096, fs.getDefaultReplication(new Path(path)), fs.getDefaultBlockSize(new Path(path)), null)
                    .close(); // force the file to exists
            return 0;
        } catch (IOException e) {
            System.err.println("Error creating file: " + path);
            return -Errno.EIO();
        }
    }

    @Override
    public int rename(String from, String to, int flags) {
        try {
            fs.rename(new Path(from), new Path(to));
            return 0;
        } catch (IOException e) {
            System.err.println("Error renaming file: " + from + " to: " + to);
            return -Errno.EIO();
        }
    }

    @Override
    public int chown(String path, long uid, long gid, FuseFileInfo fi) {
        return 0;
    }

    @Override
    public int chmod(String path, long mode, FuseFileInfo fi) {
        try {
            FsPermission permission = new FsPermission((short) (mode & 0777));
            fs.setPermission(new Path(path), permission);
            return 0;
        } catch (IOException e) {
            System.err.println("Error setting file permissions: " + path);
            return -Errno.EIO();
        }
    }

    @Override
    public int truncate(String path, long size, FuseFileInfo fi) {
        try {
            var status = fs.getFileStatus(new Path(path));
            if (status.isDirectory()) {
                return -Errno.EISDIR();
            }
            try {
                fs.truncate(new Path(path), size);
            } catch (RemoteException e) {
                // HDFS does not support truncating files remotely
            }
            return 0;
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error truncating file: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }

    @Override
    public int getxattr(String path, String name, byte[] value, long size) {
        try {
            byte[] xAttr = fs.getXAttr(new Path(path), name);
            if (xAttr.length > size) {
                return -Errno.ERANGE();
            }
            System.arraycopy(xAttr, 0, value, 0, Math.min(xAttr.length, (int) size));
            if (size > xAttr.length)
                value[xAttr.length] = 0;
            return 0;
        } catch (AccessControlException e) {
            return -Errno.EACCES();
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error getting xattr: " + name + " for file: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }

    @Override
    public int setxattr(String path, String name, byte[] value, long size, int flags) {
        try {
            XAttrSetFlag flag;
            if ((flags & 1) == 1)
                flag = XAttrSetFlag.CREATE;
            else
                flag = XAttrSetFlag.REPLACE;
            fs.setXAttr(new Path(path), name, value, EnumSet.of(flag));
            return 0;
        } catch (AccessControlException e) {
            return -Errno.EACCES();
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error setting xattr: " + name + " for file: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }

    @Override
    public int removexattr(String path, String name) {
        try {
            fs.removeXAttr(new Path(path), name);
            return 0;
        } catch (AccessControlException e) {
            return -Errno.EACCES();
        } catch (FileNotFoundException e) {
            return -Errno.ENOENT();
        } catch (IOException e) {
            System.err.println("Error removing xattr: " + name + " for file: " + path);
            e.printStackTrace();
            return -Errno.EIO();
        }
    }

    @Override
    public int flush(String path, FuseFileInfo fi) {
        long handle = fi.fh();
        FileWriteInfo writeInfo = openWriteFiles.get(handle);

        if (writeInfo != null) {
            try {
                // Use hflush to flush data to DataNodes
                writeInfo.out.hflush();
                // Alternatively, use hsync to sync data to disk
                // writeInfo.out.hsync();
                return 0;
            } catch (IOException e) {
                System.err.println("Error flushing output stream for file: " + path);
                e.printStackTrace();
                return -Errno.EIO();
            }
        }
        return 0;
    }

}
