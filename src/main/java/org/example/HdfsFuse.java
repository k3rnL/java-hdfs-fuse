package org.example;

import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class HdfsFuse extends FuseStubFS {

    final FileSystem fs;

    private final Map<Long, FileReadInfo> openFiles = new ConcurrentHashMap<>();
    private final Map<Long, FileWriteInfo> openWriteFiles = new ConcurrentHashMap<>();
    private final AtomicLong handleCounter = new AtomicLong();

    private record FileReadInfo(SeekableBufferedInputStream in, Path path) {
    }

    private static class FileWriteInfo {
        final FSDataOutputStream out;
        final Path path;
        int lastOffset;

        public FileWriteInfo(FSDataOutputStream out, Path path) {
            this.out = out;
            this.path = path;
        }
    }

    public HdfsFuse(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public int getattr(String path, FileStat stat) {
        try {
            var status = fs.getFileStatus(new Path(path));
            var permission = status.getPermission().toShort();
            stat.st_gid.set(1000);
            stat.st_uid.set(1000);
            if (status.isFile())
                stat.st_mode.set(FileStat.S_IFREG | permission);
            else if (status.isDirectory())
                stat.st_mode.set(FileStat.S_IFDIR | permission);
            stat.st_size.set(status.getLen());
            stat.st_nlink.set(1);
            stat.st_blksize.set(4096); // Preferred block size
            stat.st_blocks.set((status.getLen() + 511) / 512);
            stat.st_atim.tv_nsec.set(status.getAccessTime() * 1000);
            stat.st_atim.tv_sec.set(status.getAccessTime() / 1000);
            stat.st_mtim.tv_sec.set(status.getModificationTime() / 1000);
            stat.st_mtim.tv_nsec.set(status.getModificationTime() * 1000);
            return 0;
        } catch (FileNotFoundException e) {
            return -ErrorCodes.ENOENT();
        } catch (AccessControlException e) {
            return -ErrorCodes.EACCES();
        } catch (IOException e) {
            System.err.println("Error getting file status for path: " + path);
            e.printStackTrace();
        }
        return -ErrorCodes.ENOENT();
    }


    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) {
        try {
            var folderStatus = fs.getFileStatus(new Path(path));
            if (!folderStatus.isDirectory()) {
                return -ErrorCodes.ENOTDIR();
            }
            var status = fs.listStatus(new Path(path));
            filter.apply(buf, ".", null, 0);
            filter.apply(buf, "..", null, 0);
            for (var fileStatus : status) {
                filter.apply(buf, fileStatus.getPath().getName(), null, 0);
            }
            return 0;
        } catch (FileNotFoundException e) {
            return -ErrorCodes.ENOENT();
        } catch (IOException e) {
            System.err.println("Error listing directory: " + path);
            return -ErrorCodes.EIO();
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
            fi.fh.set(handle); // Set the file handle
            return 0;
        } catch (IOException e) {
            System.err.println("Error creating file: " + path);
            e.printStackTrace();
            return -ErrorCodes.EIO();
        }
    }



    @Override
    public int open(String path, FuseFileInfo fi) {
        Path filePath = new Path(path);
        int flags = fi.flags.get();
        int accessMode = flags & OpenFlags.O_ACCMODE.intValue();

        try {
            FileStatus status = null;
            boolean fileExists = fs.exists(filePath);
            if (fileExists) {
                status = fs.getFileStatus(filePath);
                if (status.isDirectory()) {
                    return -ErrorCodes.EISDIR();
                }
            }

            // Handle creation flags
            if ((flags & OpenFlags.O_CREAT.intValue()) != 0) {
                if (!fileExists) {
                    // Create the file
                    fs.create(filePath).close();
                    fileExists = true;
                } else {
                    if ((flags & OpenFlags.O_EXCL.intValue()) != 0) {
                        return -ErrorCodes.EEXIST();
                    }
                }
            } else {
                if (!fileExists) {
                    return -ErrorCodes.ENOENT();
                }
            }

            // Handle access modes
            long handle = handleCounter.incrementAndGet();

            if (accessMode == OpenFlags.O_RDONLY.intValue()) {
                // Open for reading
                SeekableBufferedInputStream in = new SeekableBufferedInputStream(fs.open(filePath), 2048 * 1024, 20);
                openFiles.put(handle, new FileReadInfo(in, filePath)); // can keep 20MB in memory
                fi.fh.set(handle);
            } else if (accessMode == OpenFlags.O_WRONLY.intValue() || accessMode == OpenFlags.O_RDWR.intValue()) {
                // Open for writing or reading and writing
                FileWriteInfo writeInfo;
                if ((flags & OpenFlags.O_APPEND.intValue()) != 0) {
                    // Open for appending
                    FSDataOutputStream out = fs.append(filePath);
                    writeInfo = new FileWriteInfo(out, filePath);
                    writeInfo.lastOffset = (int) status.getLen();
                } else {
                    // Overwrite the file
                    FSDataOutputStream out = fs.create(filePath, true);
                    writeInfo = new FileWriteInfo(out, filePath);
                    writeInfo.lastOffset = 0;
                }
                openWriteFiles.put(handle, writeInfo);
                fi.fh.set(handle);
            } else {
                // Unsupported access mode
                return -ErrorCodes.EACCES();
            }

            return 0;
        } catch (AccessControlException e) {
            return -ErrorCodes.EACCES();
        } catch (IOException e) {
            System.err.println("Error opening file: " + path);
            e.printStackTrace();
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int release(String path, FuseFileInfo fi) {
        long handle = fi.fh.get();

        // Close input stream if it's open
        FileReadInfo in = openFiles.remove(handle);
        if (in != null) {
            try {
                in.in.close();
            } catch (IOException e) {
                System.err.println("Error closing input stream for file: " + path);
                e.printStackTrace();
                return -ErrorCodes.EIO();
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
                return -ErrorCodes.EIO();
            }
        }

        return 0;
    }

    @Override
    public int rmdir(String path) {
        try {
            var status = fs.getFileStatus(new Path(path));
            if (!status.isDirectory()) {
                return -ErrorCodes.ENOTDIR();
            }
            fs.delete(new Path(path), true);
            return 0;
        } catch (FileNotFoundException e) {
            return -ErrorCodes.ENOENT();
        } catch (IOException e) {
            System.err.println("Error deleting directory: " + path);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int unlink(String path) {
        try {
            var status = fs.getFileStatus(new Path(path));
            if (status.isDirectory()) {
                return -ErrorCodes.EISDIR();
            }
            fs.delete(new Path(path), false);
            return 0;
        } catch (FileNotFoundException e) {
            return -ErrorCodes.ENOENT();
        } catch (IOException e) {
            System.err.println("Error deleting file: " + path);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        long handle = fi.fh.get();
        FileReadInfo info = openFiles.get(handle);
        SeekableBufferedInputStream in = info.in;

        if (in == null) {
            return -ErrorCodes.EBADF(); // Invalid file handle
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
                    buf.put(0, data, 0, totalBytesRead);
                    return totalBytesRead;
                } else {
                    return 0; // EOF
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading from file: " + path);
            e.printStackTrace();
            return -ErrorCodes.EIO();
        } catch (Exception e) {
            e.printStackTrace();
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        long handle = fi.fh.get();
        FileWriteInfo writeInfo = openWriteFiles.get(handle);

        if (writeInfo == null) {
            return -ErrorCodes.EBADF(); // Invalid file handle
        }

        try {
            if (offset < writeInfo.lastOffset) {
                // Writing before the last written offset is not supported
                return -ErrorCodes.EINVAL();
            }

            if (offset > writeInfo.lastOffset) {
                // Need to fill the gap between lastOffset and offset with zeros
                long gapSize = offset - writeInfo.lastOffset;
                byte[] zeros = new byte[(int) gapSize];
                writeInfo.out.write(zeros);
                writeInfo.lastOffset += gapSize;
            }

            byte[] data = new byte[(int) size];
            buf.get(0, data, 0, (int) size);
            writeInfo.out.write(data);
            writeInfo.lastOffset += size;

            return (int) size;
        } catch (IOException e) {
            System.err.println("Error writing to file: " + path);
            e.printStackTrace();
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int utimens(String path, Timespec[] timespec) {
        try {
            var status = fs.getFileStatus(new Path(path));
            fs.setTimes(new Path(path), timespec[0].tv_sec.get(), timespec[1].tv_sec.get());
            return 0;
        } catch (FileNotFoundException e) {
            return -ErrorCodes.ENOENT();
        } catch (IOException e) {
            System.err.println("Error setting file times: " + path);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int truncate(String path, long size) {
        try {
            var status = fs.getFileStatus(new Path(path));
            if (status.isDirectory()) {
                return -ErrorCodes.EISDIR();
            }
//            fs.truncate(new Path(path), size);
            return 0;
        } catch (FileNotFoundException e) {
            return -ErrorCodes.ENOENT();
        } catch (IOException e) {
            System.err.println("Error truncating file: " + path);
            e.printStackTrace();
            return -ErrorCodes.EIO();
        }
    }
}
