package com.k3rnl.hdfs.fuse;

import org.apache.avro.util.ByteBufferInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TestSeekableBufferedInputStream {

    public static void main(String[] args) throws IOException {
        // 128MB buffer
        byte[] data = new byte[4096 * 32768]; //32768
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // put some data in the buffer
        for (int i = 0; i < data.length / 4; i += 4) {
            buffer.putInt(i);
        }

        // check the last byte
        System.out.println(buffer.getInt(data.length - 4));

        ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
        SeekableBufferedInputStream stream = new SeekableBufferedInputStream(new ByteArrayInputStream(data));

        int bytesToRead = 262144;
        int totalRead = 0;
        while (true) {
            byte[] bufferA = new byte[bytesToRead];
            byte[] bufferB = new byte[bytesToRead];

            stream.seek(totalRead);
            int readA = byteStream.readNBytes(bufferA, 0, bytesToRead);
            int readB = stream.readNBytes(bufferB, 0, bytesToRead);

            totalRead += readA;

            if (readA != readB) {
                System.out.println("Read different number of bytes");
                break;
            }
            for (int i = 0; i < readA; i++) {
                if (bufferA[i] != bufferB[i]) {
                    System.out.println("Read different data");
                    break;
                }
            }

            if (readA < bytesToRead) {
                break;
            }
        }
    }

}
