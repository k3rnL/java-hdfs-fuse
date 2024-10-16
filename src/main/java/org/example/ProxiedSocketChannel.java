package org.example;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ProxiedSocketChannel extends SocketChannel {
    /**
     * Set of supported options.
     */
    private static final Set<SocketOption<?>> SUPPORTED_OPTIONS;

    static {
        final HashSet<SocketOption<?>> set = new HashSet<SocketOption<?>>();
        set.add(StandardSocketOptions.SO_KEEPALIVE);
        set.add(StandardSocketOptions.SO_RCVBUF);
        set.add(StandardSocketOptions.SO_SNDBUF);
        set.add(StandardSocketOptions.SO_REUSEADDR);
        set.add(StandardSocketOptions.TCP_NODELAY);
        set.add(StandardSocketOptions.SO_LINGER);
        SUPPORTED_OPTIONS = Collections.unmodifiableSet(set);
    }

    /**
     * Constant for setting socket timeout to <em>infinite</em>.
     */
    private static final int TIMEOUT_INFINITE = 0;

    /**
     * Return code for EOF (end-of-file).
     */
    private static final int EOF = -1;

    /**
     * The underlying "classic" socket which does have proxy support.
     */
    private final Socket socket;

    private volatile boolean isBlocking = true;

    /**
     * Create a proxied socket channel with provided proxy.
     *
     * @param proxy the required proxy configuration (null == Proxy.NO_PROXY)
     * @throws IOException
     */
    public ProxiedSocketChannel(final Proxy proxy) throws IOException {
        super(SelectorProvider.provider());
        if (proxy == null) {
            this.socket = new Socket(Proxy.NO_PROXY) {
                @Override
                public SocketChannel getChannel() {
                    return ProxiedSocketChannel.this;
                }
            };
        } else {
            this.socket = new Socket(proxy) {
                @Override
                public SocketChannel getChannel() {
                    return ProxiedSocketChannel.this;
                }

            };
        }
        // set timeout to 0 (infinite) since socketchannel generally does not
        // time out (no SocketOption available either)
        this.socket.setSoTimeout(TIMEOUT_INFINITE);
    }

    @Override
    public SocketAddress getLocalAddress() {
        return this.socket.getLocalSocketAddress();
    }

    @Override
    public SocketChannel bind(final SocketAddress local) throws IOException {
        this.socket.bind(local);
        return this;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return SUPPORTED_OPTIONS;
    }

    @Override
    public <T> T getOption(final SocketOption<T> option) throws IOException {
        if (StandardSocketOptions.SO_KEEPALIVE.equals(option)) {
            return (T) StandardSocketOptions.SO_KEEPALIVE.type().cast(this.socket.getKeepAlive());
        } else if (StandardSocketOptions.SO_RCVBUF.equals(option)) {
            return (T) StandardSocketOptions.SO_RCVBUF.type().cast(this.socket.getReceiveBufferSize());
        } else if (StandardSocketOptions.SO_SNDBUF.equals(option)) {
            return (T) StandardSocketOptions.SO_SNDBUF.type().cast(this.socket.getSendBufferSize());
        } else if (StandardSocketOptions.SO_REUSEADDR.equals(option)) {
            return (T) StandardSocketOptions.SO_REUSEADDR.type().cast(this.socket.getReuseAddress());
        } else if (StandardSocketOptions.TCP_NODELAY.equals(option)) {
            return (T) StandardSocketOptions.TCP_NODELAY.type().cast(this.socket.getTcpNoDelay());
        } else if (StandardSocketOptions.SO_LINGER.equals(option)) {
            return (T) StandardSocketOptions.SO_LINGER.type().cast(this.socket.getSoLinger());
        }
        throw new IllegalArgumentException("Unsupported option specified.");
    }

    @Override
    public <T> SocketChannel setOption(final SocketOption<T> option, final T value)
            throws IOException {
        if (StandardSocketOptions.SO_KEEPALIVE.equals(option)) {
            final Class<Boolean> keepaliveType = StandardSocketOptions.SO_KEEPALIVE.type();
            this.socket.setKeepAlive(keepaliveType.cast(value));
        } else if (StandardSocketOptions.SO_RCVBUF.equals(option)) {
            final Class<Integer> rcvbufType = StandardSocketOptions.SO_RCVBUF.type();
            this.socket.setReceiveBufferSize(rcvbufType.cast(value));
        } else if (StandardSocketOptions.SO_SNDBUF.equals(option)) {
            final Class<Integer> sndbufType = StandardSocketOptions.SO_SNDBUF.type();
            this.socket.setSendBufferSize(sndbufType.cast(value));
        } else if (StandardSocketOptions.SO_REUSEADDR.equals(option)) {
            final Class<Boolean> reuseType = StandardSocketOptions.SO_REUSEADDR.type();
            this.socket.setReuseAddress(reuseType.cast(value));
        } else if (StandardSocketOptions.TCP_NODELAY.equals(option)) {
            final Class<Boolean> nodelayType = StandardSocketOptions.TCP_NODELAY.type();
            this.socket.setTcpNoDelay(nodelayType.cast(value));
        } else if (StandardSocketOptions.SO_LINGER.equals(option)) {
            final Class<Integer> lingerType = StandardSocketOptions.SO_LINGER.type();
            final Integer lingerValue = lingerType.cast(value);
            final boolean enabled = lingerValue >= 0;
            this.socket.setSoLinger(enabled, lingerValue);
        }
        throw new IllegalArgumentException("Unsupported option specified.");
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        // FIXME synchronize on InputStream before shutting down?
        this.socket.shutdownInput();
        return this;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        // FIXME synchronize on OutputStream before shutting down?
        this.socket.shutdownOutput();
        return this;
    }

    @Override
    public Socket socket() {
        return this.socket;
    }

    @Override
    public boolean isConnected() {
        return this.socket.isConnected();
    }

    @Override
    public boolean isConnectionPending() {
        return false;
    }

    @Override
    public boolean connect(final SocketAddress remote) throws IOException {
        this.socket.connect(remote);
        return true;
    }

    @Override
    public boolean finishConnect() throws IOException {
        return this.socket.isConnected();
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return this.socket.getRemoteSocketAddress();
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        final InputStream input = this.socket.getInputStream();
        synchronized (input) {
            if (this.isBlocking) {
                return readBlocking(input, dst);
            } else {
                return readNonBlocking(input, dst);
            }
        }
    }

    /**
     * Read from provided input stream in blocking fashion.
     * <p>
     * NOTE: assumes that calling thread is already <em>synchronized</em> on
     * input stream!
     *
     * @param input the SYNCHRONIZED input stream
     * @param dst   destination buffer
     * @return returns number of bytes read
     * @throws IOException in case of IOException
     */
    private int readBlocking(final InputStream input, final ByteBuffer dst) throws IOException {
        final byte[] buffer = new byte[dst.remaining()];
        final int size = input.read(buffer);
        if (size > 0) {
            dst.put(buffer, 0, size);
        }
        return size;
    }

    private int readNonBlocking(final InputStream input, final ByteBuffer dst) throws IOException {
        int available = input.available();
        if (available == 0) {
            return 0;
        }
        int toRead = Math.min(dst.remaining(), available);
        byte[] buffer = new byte[toRead];
        int bytesRead = input.read(buffer);
        if (bytesRead > 0) {
            dst.put(buffer, 0, bytesRead);
        }
        return bytesRead;
    }

    @Override
    public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
        final InputStream input = this.socket.getInputStream();
        synchronized (input) {
            if (this.isBlocking) {
                return readBlocking(input, dsts, offset, length);
            } else {
                return readNonBlocking(input, dsts, offset, length);
            }
        }
    }

    private long readNonBlocking(final InputStream input, final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
        int total = 0;
        for (int i = offset; i < offset + length; i++) {
            int bytesRead = readNonBlocking(input, dsts[i]);
            if (bytesRead == 0 && total == 0) {
                return 0;
            } else if (bytesRead == -1) {
                return total == 0 ? -1 : total;
            }
            total += bytesRead;
            if (bytesRead < dsts[i].remaining()) {
                break;
            }
        }
        return total;
    }

    /**
     * Read from provided input stream in blocking fashion.
     * <p>
     * NOTE: assumes that calling thread is already <em>synchronized</em> on
     * input stream!
     *
     * @param input  the SYNCHRONIZED input stream
     * @param dsts   the array of destination buffers
     * @param offset the offset for first available buffer
     * @param length the number of buffers to use
     * @return returns number of bytes read
     * @throws IOException
     */
    private long readBlocking(final InputStream input, final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
        int total = 0;
        for (int i = offset; i < offset + length; i++) {
            final int size = readBlocking(input, dsts[i]);
            if (size == EOF) {
                if (total == 0) {
                    // Very first response is EOF. Signal EOF to reader.
                    return EOF;
                } else {
                    // EOF is not very first data read, so break and return
                    // whatever we have read so far.
                    break;
                }
            }
            total += size;
        }
        return total;
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        final OutputStream output = this.socket.getOutputStream();
        synchronized (output) {
            return writeNonBlocking(output, src);
        }
    }

    private int writeNonBlocking(final OutputStream output, final ByteBuffer src) throws IOException {
        if (src.remaining() == 0) {
            return 0;
        }
        byte[] buffer = new byte[src.remaining()];
        src.get(buffer);
        output.write(buffer);
        return buffer.length;
    }

    @Override
    public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
        final OutputStream output = this.socket.getOutputStream();
        synchronized (output) {
            long total = 0;
            for (int i = offset; i < offset + length; i++) {
                total += writeNonBlocking(output, srcs[i]);
            }
            return total;
        }
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        this.socket.close();
    }

    @Override
    protected void implConfigureBlocking(final boolean block) throws IOException {
        this.isBlocking = block;
    }
}