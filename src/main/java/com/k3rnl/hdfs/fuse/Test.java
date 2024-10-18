package com.k3rnl.hdfs.fuse;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import jnr.ffi.Pointer;
import jnr.ffi.provider.FromNativeType;
import jnr.ffi.provider.ToNativeType;
import jnr.ffi.provider.jffi.*;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;

import static jnr.ffi.provider.jffi.CodegenUtils.*;
import static jnr.ffi.provider.jffi.NumberUtil.convertPrimitive;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Opcodes.ACC_FINAL;

//@TargetClass(className = "jnr.ffi.provider.jffi.AsmClassLoader")
public class Test {

    @Alias
    private final ConcurrentMap<String, Class> definedClasses = null;

    @Substitute
    public Class defineClass(String name, byte[] b) {
        name = hashBytes(b);
        Class klass = defineClass(name, b, 0, b.length);
        definedClasses.putIfAbsent(name, klass);
        resolveClass(klass);
        return klass;
    }

    String hashBytes(byte[] b) {
        int hash = 0;
        for (byte bb : b) {
            hash = 31 * hash + bb;
        }
        return Integer.toString(hash);
    }

    @Alias
    protected final void resolveClass(Class<?> c) {}

    @Alias
    protected final Class<?> defineClass(String name, byte[] b, int off, int len)
            throws ClassFormatError { return null; }

}
