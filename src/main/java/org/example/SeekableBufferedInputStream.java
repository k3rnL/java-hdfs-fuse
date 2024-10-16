package org.example;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Seekable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SeekableBufferedInputStream extends BufferedInputStream implements Seekable {

  /**
   * Constructor.
   * @param in input stream implements seekable
   * @param size the buffer size
   */
  public SeekableBufferedInputStream(InputStream in, int size) {
    super(in, size);
    Preconditions.checkArgument(in instanceof Seekable,
        "Input stream must implement Seeakble");
  }

  @Override
  public long getPos() throws IOException {
    return ((Seekable) in).getPos() - (count - pos);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos() + n);
    return n;
  }

  @Override
  public void seek(long newPosition) throws IOException {
    if (newPosition < 0 || getPos() == newPosition) {
      return;
    }
    // optimize: check if the pos is in the buffer
    long end = ((Seekable) in).getPos();
    long start = end - count;
    if (start <= newPosition && newPosition < end) {
      pos = (int) (newPosition - start);
      return;
    }

    // invalidate buffer
    pos = 0;
    count = 0;

    System.out.println("Seeking to " + newPosition);

    ((Seekable) in).seek(newPosition);
  }
}