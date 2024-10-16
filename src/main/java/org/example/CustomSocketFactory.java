package org.example;

import org.apache.hadoop.net.StandardSocketFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CustomSocketFactory extends StandardSocketFactory {
    private final Map<HostPort, HostPort> addressMapping;

    public CustomSocketFactory() {
        this.addressMapping = new HashMap<>();
        addressMapping.put(new HostPort("big-datanode6.bigdata.cls.fr", 50010), new HostPort("localhost", 50010));
        addressMapping.put(new HostPort("192.168.97.16", 50010), new HostPort("localhost", 50010));
        addressMapping.put(new HostPort("big-datanode7.bigdata.cls.fr", 50010), new HostPort("localhost", 50011));
        addressMapping.put(new HostPort("192.168.97.17", 50010), new HostPort("localhost", 50011));
        addressMapping.put(new HostPort("big-datanode8.bigdata.cls.fr", 50010), new HostPort("localhost", 50012));
        addressMapping.put(new HostPort("192.168.97.18", 50010), new HostPort("localhost", 50012));
    }

    @Override
    public Socket createSocket() throws IOException {
        return new MappedSocket();
    }

    // Other createSocket methods can be overridden similarly

    private class MappedSocket extends Socket {
        private final SocketChannel channel;
        private SocketAddress originalAddress;

        public MappedSocket() throws IOException {
            this.channel = new MappedSocketChannel(SocketChannel.open(), addressMapping);
//            this.channel.configureBlocking(false);
        }

        @Override
        public void connect(SocketAddress endpoint) throws IOException {
            connect(endpoint, 0);
        }

        @Override
        public SocketChannel getChannel() {
            return null;
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            this.originalAddress = endpoint;
            HostPort original = toHostPort(endpoint);
            HostPort mapped = addressMapping.get(original);
            if (mapped == null) {
                System.out.println("Direct connection to " + original);
                channel.socket().connect(endpoint, timeout);
                return;
            }

            SocketAddress mappedEndpoint = new InetSocketAddress(mapped.host, mapped.port);
            System.out.println("Connecting to " + mappedEndpoint);
            channel.socket().connect(mappedEndpoint, 1000);
            System.out.println("Connected to " + mappedEndpoint);
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return channel.socket().getInputStream();
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            return channel.socket().getOutputStream();
        }

        @Override
        public boolean isConnected() {
            return channel.isConnected();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        @Override
        public InetAddress getInetAddress() {
            return ((InetSocketAddress) originalAddress).getAddress();
        }

        @Override
        public SocketAddress getLocalSocketAddress() {
            return channel.socket().getLocalSocketAddress();
        }

        private HostPort toHostPort(SocketAddress address) {
            if (address instanceof InetSocketAddress) {
                InetSocketAddress inetAddr = (InetSocketAddress) address;
                return new HostPort(inetAddr.getHostString(), inetAddr.getPort());
            }
            throw new IllegalArgumentException("Unsupported SocketAddress type");
        }
    }

    private static class HostPort {
        public final String host;
        public final int port;

        public HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof HostPort)) return false;
            HostPort hostPort = (HostPort) o;
            return port == hostPort.port && host.equals(hostPort.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    private static class MappedSocketChannel extends SocketChannel {
        private final SocketChannel innerChannel;
        private final Map<HostPort, HostPort> addressMapping;

        public MappedSocketChannel(SocketChannel channel, Map<HostPort, HostPort> addressMapping) throws IOException {
            super(channel.provider());
            this.innerChannel = channel;
            this.addressMapping = addressMapping;
        }

        @Override
        public boolean connect(SocketAddress remote) throws IOException {
            HostPort original = toHostPort(remote);
            HostPort mapped = addressMapping.get(original);
            if (mapped == null) {
                System.out.println("Direct connection to " + original);
                return innerChannel.connect(remote);
            }

            System.out.println("Connecting to " + mapped.host + ":" + mapped.port);
            SocketAddress mappedEndpoint = new InetSocketAddress(mapped.host, mapped.port);
            boolean result = innerChannel.connect(mappedEndpoint);
            System.out.println("Connected to " + mapped.host + ":" + mapped.port);
            return result;
        }

        @Override
        public boolean finishConnect() throws IOException {
            return innerChannel.finishConnect();
        }

        @Override
        public SocketChannel bind(SocketAddress local) throws IOException {
            innerChannel.bind(local);
            return this;
        }
        @Override
        public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
            return innerChannel.setOption(name, value);
        }
        @Override
        public SocketChannel shutdownInput() throws IOException {
            return innerChannel.shutdownInput();
        }
        @Override
        public SocketChannel shutdownOutput() throws IOException {
            return innerChannel.shutdownOutput();
        }

        @Override
        public SocketAddress getLocalAddress() throws IOException {
            return innerChannel.getLocalAddress();
        }

        @Override
        public <T> T getOption(SocketOption<T> name) throws IOException {
            return innerChannel.getOption(name);
        }

        @Override
        public Set<SocketOption<?>> supportedOptions() {
            return innerChannel.supportedOptions();
        }

        @Override
        public boolean isConnected() {
            return innerChannel.isConnected();
        }

        @Override
        public boolean isConnectionPending() {
            return innerChannel.isConnectionPending();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return innerChannel.read(dst);
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return innerChannel.read(dsts, offset, length);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return innerChannel.write(src);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return innerChannel.write(srcs, offset, length);
        }

        @Override
        public SocketAddress getRemoteAddress() throws IOException {
            return innerChannel.getRemoteAddress();
        }

        @Override
        protected void implCloseSelectableChannel() throws IOException {
            innerChannel.close();
        }

        @Override
        protected void implConfigureBlocking(boolean block) throws IOException {
            innerChannel.configureBlocking(block);
        }

        @Override
        public Socket socket() {
            return innerChannel.socket();
        }

        private HostPort toHostPort(SocketAddress address) {
            if (address instanceof InetSocketAddress) {
                InetSocketAddress inetAddr = (InetSocketAddress) address;
                return new HostPort(inetAddr.getHostString(), inetAddr.getPort());
            }
            throw new IllegalArgumentException("Unsupported SocketAddress type: " + address.getClass());
        }
    }
}