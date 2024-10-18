package com.k3rnl.hdfs.fuse;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.mapper.FromNativeConverter;
import jnr.ffi.provider.jffi.ClosureHelper;
import jnr.posix.util.Platform;
import ru.serce.jnrfuse.*;
import ru.serce.jnrfuse.struct.*;
import ru.serce.jnrfuse.utils.MountUtils;
import ru.serce.jnrfuse.utils.SecurityUtils;
import ru.serce.jnrfuse.utils.WinPathUtils;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class AbstractFuseFS implements FuseFS {

    private static final int TIMEOUT = 2000; // ms
    private static final String PROPERTY_WINLIB_PATH = "jnr-fuse.windows.libpath";
    private static final String[] osxFuseLibraries = {"fuse4x", "osxfuse", "macfuse", "fuse"};

    private Set<String> notImplementedMethods;
    protected final LibFuse libFuse;
    protected final FuseOperations fuseOperations;
    protected final AtomicBoolean mounted = new AtomicBoolean();
    protected Path mountPoint;
    private volatile Pointer fusePointer;

    public AbstractFuseFS() {
        jnr.ffi.Platform p = jnr.ffi.Platform.getNativePlatform();
        LibFuse libFuse = null;
        switch (p.getOS()) {
            case DARWIN:
                for (String library : osxFuseLibraries) {
                    try {
                        // Regular FUSE-compatible fuse library
                        libFuse = LibraryLoader.create(LibFuse.class)
                            .failImmediately()
                            // adds user library search path which is not included by default
                            .search("/usr/local/lib/")
                            .load(library);
                        break;
                    } catch (Throwable e) {
                        // Carry on
                    }
                }
                if (libFuse == null) {
                    // Everything failed. Do a last-ditch attempt.
                    // Worst-case scenario, this causes an exception
                    // which will be more meaningful to the user than a NullPointerException on libFuse.
                    libFuse = LibraryLoader.create(LibFuse.class).failImmediately().load("fuse");
                }
                break;
            case WINDOWS:
                //see if the property is set, otherwise use winfsp
                String windowsLibPath = System.getProperty(PROPERTY_WINLIB_PATH, WinPathUtils.getWinFspPath());
                libFuse = LibraryLoader.create(LibFuse.class).failImmediately().load(windowsLibPath);
                break;
            case LINUX:
            default: // Assume linux since we have no further OS evidence
                try {
                    // Try loading library by going through the library mapping process, see j.l.System.mapLibraryName
                    libFuse = LibraryLoader.create(LibFuse.class).failImmediately().load("fuse");
                } catch (Throwable e) {
                    // Try loading the dynamic library directly which will end up calling dlopen directly, see
                    // com.kenai.jffi.Foreign.dlopen
                    libFuse = LibraryLoader.create(LibFuse.class).failImmediately().load("libfuse.so.2");
                }
        }
        this.libFuse = libFuse;

        Runtime runtime = Runtime.getSystemRuntime();
        fuseOperations = new FuseOperations(runtime);
        init(fuseOperations);
    }

    public FuseContext getContext() {
        return libFuse.fuse_get_context();
    }

    int _getattr(String path, Pointer stat) {
        return this.getattr(path, FileStat.of(stat));
    }

    int _open(String path, Pointer fi) {
        return this.open(path, FuseFileInfo.of(fi));
    }

    int _read(String path, Pointer buf, long size, long offset, Pointer fi) {
        return this.read(path, buf, size, offset, FuseFileInfo.of(fi));
    }

    int _write(String path, Pointer buf, long size, long offset, Pointer fi) {
        return this.write(path, buf, size, offset, FuseFileInfo.of(fi));
    }

    int _statfs(String path, Pointer stbuf) {
        return this.statfs(path, Statvfs.of(stbuf));
    }

    int _flush(String path, Pointer fi) {
        return this.flush(path, FuseFileInfo.of(fi));
    }

    int _release(String path, Pointer fi) {
        return this.release(path, FuseFileInfo.of(fi));
    }

    int _fsync(String path, int isdatasync, Pointer fi) {
        return this.fsync(path, isdatasync, FuseFileInfo.of(fi));
    }

    int _setxattr(String path, String name, Pointer value, long size, int flags) {
        return this.setxattr(path, name, value, size, flags);
    }

    int _getxattr(String path, String name, Pointer value, long size) {
        return this.getxattr(path, name, value, size);
    }

    int _listxattr(String path, Pointer list, long size) {
        return this.listxattr(path, list, size);
    }

    int _removexattr(String path, String name) {
        return this.removexattr(path, name);
    }

    int _opendir(String path, Pointer fi) {
        return this.opendir(path, FuseFileInfo.of(fi));
    }

    int _readdir(String path, Pointer buf, Pointer filter, long offset, Pointer fi) {
        ClosureHelper helper = ClosureHelper.getInstance();
        FromNativeConverter<FuseFillDir, Pointer> conveter = helper.getNativeConveter(FuseFillDir.class);
        FuseFillDir filterFunc = conveter.fromNative(filter, helper.getFromNativeContext());
        return this.readdir(path, buf, filterFunc, offset, FuseFileInfo.of(fi));
    }

    int _releasedir(String path, Pointer fi) {
        return this.releasedir(path, FuseFileInfo.of(fi));
    }

    int _fsyncdir(String path, Pointer fi) {
        return this.fsyncdir(path, FuseFileInfo.of(fi));
    }

    Pointer _init(Pointer conn) {
        return this.init(conn);
    }

    int _create(String path, long mode, Pointer fi) {
        return this.create(path, mode, FuseFileInfo.of(fi));
    }

    int _ftruncate(String path, long size, Pointer fi) {
        return this.ftruncate(path, size, FuseFileInfo.of(fi));
    }

    int _fgetattr(String path, Pointer stat, Pointer fi) {
        return this.fgetattr(path, FileStat.of(stat), FuseFileInfo.of(fi));
    }

    int _lock(String path, Pointer fi, int cmd, Pointer flock) {
        return this.lock(path, FuseFileInfo.of(fi), cmd, Flock.of(flock));
    }

    int _utimens(String path, Pointer timespec) {
        Timespec timespec1 = Timespec.of(timespec);
        Timespec timespec2 = Timespec.of(timespec.slice(Struct.size(timespec1)));
        return this.utimens(path, new Timespec[]{timespec1, timespec2});
    }

    int _bmap(String path, long blocksize, Pointer idx) {
        return this.bmap(path, blocksize, idx.getLong(0));
    }

    int _ioctl(String path, int cmd, Pointer arg, Pointer fi, long flags, Pointer data) {
        return this.ioctl(path, cmd, arg, FuseFileInfo.of(fi), flags, data);
    }

    int _poll(String path, Pointer fi, Pointer ph, Pointer reventsp) {
        return this.poll(path, FuseFileInfo.of(fi), FusePollhandle.of(ph), reventsp);
    }

    int _write_buf(String path, Pointer buf, long off, Pointer fi) {
        return this.write_buf(path, FuseBufvec.of(buf), off, FuseFileInfo.of(fi));
    }

    int _read_buf(String path, Pointer bufp, long size, long off, Pointer fi) {
        return this.read_buf(path, bufp, size, off, FuseFileInfo.of(fi));
    }

    int _flock(String path, Pointer fi, int op) {
        return this.flock(path, FuseFileInfo.of(fi), op);
    }

    int _fallocate(String path, int mode, long off, long length, Pointer fi) {
        return this.fallocate(path, mode, off, length, FuseFileInfo.of(fi));
    }

    private void init(FuseOperations fuseOperations) {
        notImplementedMethods = Arrays.stream(getClass().getMethods())
            .filter(method -> method.getAnnotation(NotImplemented.class) != null)
            .map(Method::getName)
            .collect(Collectors.toSet());

        AbstractFuseFS fuse = this;
        if (isImplemented("getattr")) {
            fuseOperations.getattr.set(this::_getattr);
        }
        if (isImplemented("readlink")) {
            fuseOperations.readlink.set(fuse::readlink);
        }
        if (isImplemented("mknod")) {
            fuseOperations.mknod.set(fuse::mknod);
        }
        if (isImplemented("mkdir")) {
            fuseOperations.mkdir.set(fuse::mkdir);
        }
        if (isImplemented("unlink")) {
            fuseOperations.unlink.set(fuse::unlink);
        }
        if (isImplemented("rmdir")) {
            fuseOperations.rmdir.set(fuse::rmdir);
        }
        if (isImplemented("symlink")) {
            fuseOperations.symlink.set(fuse::symlink);
        }
        if (isImplemented("rename")) {
            fuseOperations.rename.set(fuse::rename);
        }
        if (isImplemented("link")) {
            fuseOperations.link.set(fuse::link);
        }
        if (isImplemented("chmod")) {
            fuseOperations.chmod.set(fuse::chmod);
        }
        if (isImplemented("chown")) {
            fuseOperations.chown.set(fuse::chown);
        }
        if (isImplemented("truncate")) {
            fuseOperations.truncate.set(fuse::truncate);
        }
        if (isImplemented("open")) {
            fuseOperations.open.set(this::_open);
        }
        if (isImplemented("read")) {
            fuseOperations.read.set(this::_read);
        }
        if (isImplemented("write")) {
            fuseOperations.write.set(this::_write);
        }
        if (isImplemented("statfs")) {
            fuseOperations.statfs.set(this::_statfs);
        }
        if (isImplemented("flush")) {
            fuseOperations.flush.set(this::_flush);
        }
        if (isImplemented("release")) {
            fuseOperations.release.set(this::_release);
        }
        if (isImplemented("fsync")) {
            fuseOperations.fsync.set(this::_fsync);
        }
        if (isImplemented("setxattr")) {
            fuseOperations.setxattr.set(fuse::setxattr);
        }
        if (isImplemented("getxattr")) {
            fuseOperations.getxattr.set(fuse::getxattr);
        }
        if (isImplemented("listxattr")) {
            fuseOperations.listxattr.set(fuse::listxattr);
        }
        if (isImplemented("removexattr")) {
            fuseOperations.removexattr.set(fuse::removexattr);
        }
        if (isImplemented("opendir")) {
            fuseOperations.opendir.set(this::_opendir);
        }
        if (isImplemented("readdir")) {
            fuseOperations.readdir.set(this::_readdir);
        }
//            fuseOperations.readdir.set((path, buf, filter, offset, fi) -> {
//                ClosureHelper helper = ClosureHelper.getInstance();
//                FromNativeConverter<FuseFillDir, Pointer> conveter = helper.getNativeConveter(FuseFillDir.class);
//                FuseFillDir filterFunc = conveter.fromNative(filter, helper.getFromNativeContext());
//                return fuse.readdir(path, buf, filterFunc, offset, FuseFileInfo.of(fi));
//           });

        if (isImplemented("releasedir")) {
            fuseOperations.releasedir.set(this::_releasedir);
        }
        if (isImplemented("fsyncdir")) {
            fuseOperations.fsyncdir.set(this::_fsyncdir);
        }
        fuseOperations.init.set(this::_init);
//        fuseOperations.init.set(conn -> {
//            AbstractFuseFS.this.fusePointer = libFuse.fuse_get_context().fuse.get();
//            if (isImplemented("init")) {
//                return fuse.init(conn);
//            }
//            return null;
//        });
        if (isImplemented("destroy")) {
            fuseOperations.destroy.set(fuse::destroy);
        }
        if (isImplemented("access")) {
            fuseOperations.access.set(fuse::access);
        }
        if (isImplemented("create")) {
            fuseOperations.create.set(this::_create);
        }
        if (isImplemented("ftruncate")) {
            fuseOperations.ftruncate.set(this::_ftruncate);
        }
        if (isImplemented("fgetattr")) {
            fuseOperations.fgetattr.set(this::_fgetattr);
        }
        if (isImplemented("lock")) {
            fuseOperations.lock.set(this::_lock);
        }
        if (isImplemented("utimens")) {
            fuseOperations.utimens.set(this::_utimens);
        }
        if (isImplemented("bmap")) {
            fuseOperations.bmap.set(this::_bmap);
        }
        if (isImplemented("ioctl")) {
            fuseOperations.ioctl.set(this::_ioctl);
        }
        if (isImplemented("poll")) {
            fuseOperations.poll.set(this::_poll);
        }
        if (isImplemented("write_buf")) {
            fuseOperations.write_buf.set(this::_write_buf);
        }
        if (isImplemented("read_buf")) {
            fuseOperations.read_buf.set(this::_read_buf);
        }
        if (isImplemented("flock")) {
            fuseOperations.flock.set(this::_flock);
        }
        if (isImplemented("fallocate")) {
            fuseOperations.fallocate.set(this::_fallocate);
        }
    }

    @Override
    public void mount(Path mountPoint, boolean blocking, boolean debug, String[] fuseOpts) {
        if (!mounted.compareAndSet(false, true)) {
            throw new FuseException("Fuse fs already mounted!");
        }
        this.mountPoint = mountPoint;
        String[] arg;
        String mountPointStr = mountPoint.toAbsolutePath().toString();
        if (mountPointStr.endsWith("\\")) {
            mountPointStr = mountPointStr.substring(0, mountPointStr.length() - 1);
        }
        if (!debug) {
            arg = new String[]{getFSName(), "-f", mountPointStr};
        } else {
            arg = new String[]{getFSName(), "-f", "-d", mountPointStr};
        }
        if (fuseOpts.length != 0) {
            int argLen = arg.length;
            arg = Arrays.copyOf(arg, argLen + fuseOpts.length);
            System.arraycopy(fuseOpts, 0, arg, argLen, fuseOpts.length);
        }

        final String[] args = arg;
        try {
            if (SecurityUtils.canHandleShutdownHooks()) {
                java.lang.Runtime.getRuntime().addShutdownHook(new Thread(this::umount));
            }
            int res;
            if (blocking) {
                res = execMount(args);
            } else {
                // Create a separate thread to hold the mounted FUSE file system.
                CompletableFuture<Integer> mountResult = new CompletableFuture<>();
                Thread mountThread = new Thread(() -> {
                    try {
                        mountResult.complete(execMount(args));
                    } catch (Throwable t) {
                        mountResult.completeExceptionally(t);
                    }
                }, "jnr-fuse-mount-thread");
                mountThread.setDaemon(true);
                mountThread.start();
                try {
                    res = mountResult.get(TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    // ok
                    res = 0;
                }
            }
            if (res != 0) {
                throw new FuseException("Unable to mount FS, return code = " + res);
            }
        } catch (Exception e) {
            mounted.set(false);
            throw new FuseException("Unable to mount FS", e);
        }
    }

    private boolean isImplemented(String funcName) {
        return !notImplementedMethods.contains(funcName);
    }

    private int execMount(String[] arg) {
        return libFuse.fuse_main_real(arg.length, arg, fuseOperations, Struct.size(fuseOperations), null);
    }

    @Override
    public void umount() {
        if (!mounted.get()) {
            return;
        }
        if (Platform.IS_WINDOWS) {
            Pointer fusePointer = this.fusePointer;
            if (fusePointer != null) {
                libFuse.fuse_exit(fusePointer);
            }
        } else {
            MountUtils.umount(mountPoint);
        }
        mounted.set(false);
    }

    protected String getFSName() {
        return "fusefs" + ThreadLocalRandom.current().nextInt();
    }
}
