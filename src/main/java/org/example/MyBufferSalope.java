package org.example;

import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class MyBufferSalope extends InputStream implements Seekable {

    private static class StreamPart {
        final byte[] buffer;
        final int offset;
        int readBytes;

        public StreamPart(byte[] buffer, int offset) {
            this.buffer = buffer;
            this.offset = offset;
            this.readBytes = 0;
        }

    }

    final InputStream in;
    final List<StreamPart> parts = new ArrayList<>();
    final List<Integer> partsMarkedForPruning = new ArrayList<>();

    final int bufferSize;
    final int capacity;

    long position = 0;

    public MyBufferSalope(InputStream in, int bufferSize, int capacity) {
        this.in = in;
        this.bufferSize = bufferSize;
        this.capacity = capacity;
    }

    public MyBufferSalope(InputStream in) {
        this(in, 1024 * 2048, 1024);
    }

    private void pruneParts() {
        if (partsMarkedForPruning.isEmpty()) {
            return;
        }

        for (var index : partsMarkedForPruning) {
            parts.set(index, null);
        }

        partsMarkedForPruning.clear();
    }

    private StreamPart nextPart() throws IOException {
        pruneParts();

        byte[] buffer = new byte[bufferSize];
        int read = in.readNBytes(buffer, 0, bufferSize);
        if (read == -1) {
            return null; // EOF
        }
        parts.add(new StreamPart(buffer, read));
        return parts.get(parts.size() - 1);
    }

    private StreamPart loadPart(int index) throws IOException {
        Seekable in = (Seekable) this.in;
        long pos = in.getPos();
        in.seek(index * bufferSize);
        byte[] buffer = new byte[bufferSize];
        int read = this.in.readNBytes(buffer, 0, bufferSize);
        in.seek(pos);
        if (read == -1) {
            return null; // EOF
        }
        parts.set(index, new StreamPart(buffer, read));
        return parts.get(index);
    }

    private StreamPart getPart(int index) throws IOException {
        if (parts.size() == index) {
            return nextPart();
        } else if (parts.get(index) == null) {
            return loadPart(index);
        }

        return parts.get(index);
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int read = read(b, 0, 1);
        return (read == -1 ? -1 : b[0]) & 0xFF;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return read(b, off, len);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int partIndex = (int) (position / bufferSize);

        if (partIndex >= parts.size()) {
            if (nextPart() == null) {
                return -1; // EOF
            }
        }

        StreamPart part = getPart(partIndex);
        if (part == null) {
            return -1; // EOF
        }

        int read = 0;
        int remaining = len;
        while (read < len) {
            int partOffset = (int) position % bufferSize;
            int partRemaining = part.buffer.length - partOffset;
            int toRead = Math.min(partRemaining, remaining);
            System.arraycopy(part.buffer, partOffset, b, off + read, toRead);
            read += toRead;
            remaining -= toRead;
            position += toRead;
            part.readBytes += toRead;
            if (part.readBytes == part.buffer.length) {
                partsMarkedForPruning.add(partIndex);
            }
            if (remaining == 0) {
                break; // Done
            }
            if (part.buffer.length < bufferSize) { // last part
                break; // EOF
            }
            partIndex++;
            part = nextPart(); // Load next part as len > partRemaining
            if (part == null) {
                break; // EOF
            }
        }

        return read;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new IOException("Invalid position: " + pos);
        }
        if (position == pos) {
            return;
        }
        position = pos;
        int index = (int) (position / bufferSize);
        if (index >= parts.size() || parts.get(index) == null)
            getPart(index);
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

}
