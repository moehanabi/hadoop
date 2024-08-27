/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.compress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.StreamCapabilitiesPolicy;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.StringUtils;

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * CompressInputStream decompresss data. It is not thread-safe. AES CTR mode is
 * required in order to ensure that the plain text and cipher text have a 1:1
 * mapping. The decompression is buffer based. The key points of the decompression
 * are (1) calculating the counter and (2) padding through stream position:
 * <p>
 * counter = base + pos/(algorithm blocksize);
 * padding = pos%(algorithm blocksize);
 * <p>
 * The underlying stream offset is maintained as state.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class CompressInputStream extends FilterInputStream implements Seekable, PositionedReadable, HasFileDescriptor, CanSetDropBehind, CanSetReadahead, CanUnbuffer, StreamCapabilities, IOStatisticsSource {
  private final byte[] oneByteBuf = new byte[1];
  private final CompressionCodec codec;
  private final Decompressor decompressor;
  private final int bufferSize;
  private final boolean isByteBufferReadable;
  private final boolean isReadableByteChannel;
  /**
   * Decompressor pool
   */
  private final Queue<Decompressor> decompressorPool =
      new ConcurrentLinkedQueue<>();
  /** Byte Array pool */
  private final Queue<byte[]> byteArrayPool =
      new ConcurrentLinkedQueue<>();
  /**
   * Input data buffer. The data starts at inBuffer.position() and ends at
   * to inBuffer.limit().
   */
  private ByteBuffer inBuffer;
  /**
   * The decompressed data buffer. The data starts at outBuffer.position() and
   * ends at outBuffer.limit();
   */
  private ByteBuffer outBuffer;
  /**
   * Whether the underlying stream supports
   * {@link org.apache.hadoop.fs.ByteBufferReadable}
   */
  private Boolean usingByteBufferRead = null;
  private boolean closed;
  private ArrayList<Long> uncompressedIndexes = new ArrayList<>();
  private ArrayList<Long> compressedIndexes = new ArrayList<>();
  private long currentUncompressedIndex = 0;
  private long currentCompressedIndex = 0;
  private byte[] tmpBuf;

  public CompressInputStream(InputStream in, CompressionCodec codec,
                             int bufferSize, ArrayList<Long> uncompressedIndexes, ArrayList<Long> compressedIndexes) throws IOException {
    this(in, codec, bufferSize, 0, uncompressedIndexes, compressedIndexes);
  }

  public CompressInputStream(InputStream in, CompressionCodec codec,
                             int bufferSize, long streamOffset, ArrayList<Long> uncompressedIndexes, ArrayList<Long> compressedIndexes) throws IOException {
    super(in);
    this.bufferSize = bufferSize + 8192;
    this.codec = codec;
    isByteBufferReadable = in instanceof ByteBufferReadable;
    isReadableByteChannel = in instanceof ReadableByteChannel;
    inBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    outBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    decompressor = getDecompressor();

    this.uncompressedIndexes = uncompressedIndexes;
    this.compressedIndexes = compressedIndexes;
    resetStreamOffset(streamOffset);
  }

  private void resetStreamOffset(long streamOffset) throws IOException {
    // find out the streamOffset is between which two uncompressedIndexes
    currentCompressedIndex = getCompressedIndexBefore(streamOffset);
    currentUncompressedIndex = getUncompressedIndexBefore(currentCompressedIndex);
    final int pos = (int) (streamOffset - currentUncompressedIndex);
    try {
      ((Seekable) in).seek(currentCompressedIndex);
    } catch (IOException e) {
      throw new RuntimeException("Error seeking to position: " + currentCompressedIndex);
    }
    int n = readAndDecompress();
    if (n <= 0) {
      outBuffer.limit(0);
    } else {
      outBuffer.position(pos);
    }
  }

  public InputStream getWrappedStream() {
    return in;
  }

  // before contains equal
  private long getUncompressedIndexBefore(long compressedIndex) {
    int i = Collections.binarySearch(compressedIndexes, compressedIndex);
    if (i < 0) {
      i = -i - 2;
    }
    return i < 0 ? -1L : uncompressedIndexes.get(i);
  }

  // before contains equal
  private long getCompressedIndexBefore(long uncompressedIndex) {
    int i = Collections.binarySearch(uncompressedIndexes, uncompressedIndex);
    if (i < 0) {
      i = -i - 2;
    }
    return i < 0 ? -1L : compressedIndexes.get(i);
  }

  // after does not contain equal
  private long getUncompressedIndexAfter(long compressedIndex) {
    int i = Collections.binarySearch(compressedIndexes, compressedIndex);
    if (i < 0) {
      i = -i - 1;
    } else {
      i++;
    }
    return i >= uncompressedIndexes.size() ? -1L : uncompressedIndexes.get(i);
  }

  // after does not contain equal
  private long getCompressedIndexAfter(long uncompressedIndex) {
    int i = Collections.binarySearch(uncompressedIndexes, uncompressedIndex);
    if (i < 0) {
      i = -i - 1;
    } else {
      i++;
    }
    return i >= compressedIndexes.size() ? -1L : compressedIndexes.get(i);
  }

  /**
   * Decryption is buffer based.
   * If there is data in {@link #outBuffer}, then read it out of this buffer.
   * If there is no data in {@link #outBuffer}, then read more from the
   * underlying stream and do the decompression.
   *
   * @param b   the buffer into which the decompressed data is read.
   * @param off the buffer offset.
   * @param len the maximum number of decompressed data bytes to read.
   * @return int the total number of decompressed data bytes read into the buffer.
   * @throws IOException raised on errors performing I/O.
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkStream();
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    final int remaining = outBuffer.remaining();
    if (remaining > 0) {
      int n = Math.min(len, remaining);
      outBuffer.get(b, off, n);
      return n;
    } else {
      int n;
      if ((n = readAndDecompress()) <= 0) {
        return n;
      }
      n = Math.min(len, outBuffer.remaining());
      outBuffer.get(b, off, n);
      return n;
    }
  }

  /*
   * Read a whole segment that can be decompressed and decompressed into buffer
   */
  private int readAndDecompress() throws IOException {
    int n = 0;

    inBuffer.clear();
    outBuffer.clear();
    // Set inBuffer limit to next compressed index
    final int toRead = (int) (getCompressedIndexAfter(currentUncompressedIndex) - currentCompressedIndex);
    if (toRead > 0) {
      inBuffer.limit(toRead);
    }

    /*
     * Check whether the underlying stream is {@link ByteBufferReadable},
     * it can avoid bytes copy.
     */
    if (usingByteBufferRead == null) {
      if (isByteBufferReadable || isReadableByteChannel) {
        try {
          while (n >= 0 && inBuffer.hasRemaining()) {
            n += isByteBufferReadable ?
                ((ByteBufferReadable) in).read(inBuffer) :
                ((ReadableByteChannel) in).read(inBuffer);
          }
          usingByteBufferRead = Boolean.TRUE;
        } catch (UnsupportedOperationException e) {
          usingByteBufferRead = Boolean.FALSE;
        }
      } else {
        usingByteBufferRead = Boolean.FALSE;
      }
      if (!usingByteBufferRead) {
        n = readFromUnderlyingStream(inBuffer);
      }
    } else {
      if (usingByteBufferRead) {
        while (n >= 0 && inBuffer.hasRemaining()) {
          n += isByteBufferReadable ? ((ByteBufferReadable) in).read(inBuffer) :
              ((ReadableByteChannel) in).read(inBuffer);
        }
      } else {
        n = readFromUnderlyingStream(inBuffer);
      }
    }
    if (n <= 0) {
      outBuffer.limit(0);
      return n;
    }

    final int originalBytes = (int) (getUncompressedIndexAfter(currentCompressedIndex) - currentUncompressedIndex);
    if (originalBytes == toRead) {
      // The data is not compressed, just swap the buffers.
      ByteBuffer temp = inBuffer;
      inBuffer = outBuffer;
      outBuffer = temp;

      inBuffer.clear();
      outBuffer.flip();
    } else {
      // The data is compressed.
      decompress(decompressor, inBuffer, outBuffer);
    }

    currentCompressedIndex += n;
    currentUncompressedIndex += outBuffer.remaining();

    return n;
  }

  /** Read data from underlying stream. */
  private int readFromUnderlyingStream(ByteBuffer inBuffer) throws IOException {
    final int toRead = inBuffer.remaining();
    final byte[] tmp = getTmpBuf();
    int n = 0;
    while (n >= 0 && inBuffer.hasRemaining()) {
      n += in.read(tmp, n, toRead - n);
    }
    if (n > 0) {
      inBuffer.put(tmp, 0, n);
    }
    return n;
  }

  private byte[] getTmpBuf() {
    if (tmpBuf == null) {
      tmpBuf = new byte[bufferSize];
    }
    return tmpBuf;
  }

  /**
   * Do the decompression using inBuffer as input and outBuffer as output.
   * Upon return, inBuffer is cleared; the decompressed data starts at
   * outBuffer.position() and ends at outBuffer.limit();
   */
  private void decompress(Decompressor decompressor, ByteBuffer inBuffer,
                          ByteBuffer outBuffer) throws IOException {
    inBuffer.flip();
    outBuffer.clear();
    final byte[] inBufferArray = getByteArray();
    final byte[] outBufferArray = getByteArray();

    final int compressedBytes = inBuffer.remaining();
    inBuffer.get(inBufferArray, 0, compressedBytes);
    final int uncompressedBytes = decompress(decompressor, inBufferArray, outBufferArray, compressedBytes);
    outBuffer.put(outBufferArray, 0, uncompressedBytes);
    inBuffer.clear();
    outBuffer.flip();

    returnByteArray(inBufferArray);
    returnByteArray(outBufferArray);
  }

  private int decompress(Decompressor decompressor, byte[] inBuffer,
                         byte[] outBuffer, int compressedBytes) throws IOException {
    decompressor.reset();
    decompressor.setInput(inBuffer, 0, compressedBytes);

    int uncompressedLen;
    int totalUncompressedLen = 0;
    while ((uncompressedLen = decompressor.decompress(outBuffer, totalUncompressedLen, outBuffer.length - totalUncompressedLen)) > 0) {
      totalUncompressedLen += uncompressedLen;
    }
    return totalUncompressedLen;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    decompressor.end();
    closed = true;
  }

  /** Positioned read. It is thread-safe */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkStream();
    if (!(in instanceof PositionedReadable)) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
          + " does not support positioned read.");
    }

    byte[] compressedBuffer = getByteArray();
    byte[] decompressedBuffer = getByteArray();
    Decompressor decompressor = getDecompressor();

    int n = 0;
    while (n >= 0 && n < length) {
      decompressor.reset();
      long beginCompressed = getCompressedIndexBefore(position + n);
      long endCompressed = getCompressedIndexAfter(position + n);
      long beginUncompressed = getUncompressedIndexBefore(beginCompressed);
      long endUncompressed = getUncompressedIndexBefore(endCompressed);
      long compressedLength = endCompressed - beginCompressed;
      long uncompressedLength = endUncompressed - beginUncompressed;

      if (compressedLength <= 0) {
        break;
      }
      if (compressedLength == uncompressedLength) {
        // data is not compressed
        long len;
        if (beginUncompressed < position) {
          // position is in the middle of the uncompressed data
          final long start = beginCompressed + (position - beginUncompressed);
          len = Math.min(uncompressedLength - (position - beginUncompressed), length - n);
          len = ((PositionedReadable) in).read(start, buffer, offset + n, (int) len);
        } else {
          len = Math.min(uncompressedLength, length - n);
          len = ((PositionedReadable) in).read(beginCompressed, buffer, offset + n, (int) len);
        }
        if (len <= 0) {
          break;
        }
        n += len;
        continue;
      }

      int len = ((PositionedReadable) in).read(beginCompressed, compressedBuffer, 0, (int) compressedLength);
      if (len <= 0) {
        break;
      }
      decompressor.reset();
      decompressor.setInput(compressedBuffer, 0, len);
      if (beginUncompressed < position) {
        // drop some unnecessary data
        long unnecessaryBytes = position - beginUncompressed;
        while (unnecessaryBytes > 0) {
          int uncompressedBytes = decompressor.decompress(decompressedBuffer, 0, (int) unnecessaryBytes);
          unnecessaryBytes -= uncompressedBytes;
        }
      }
      int uncompressedBytes;
      while (length > n && (uncompressedBytes = decompressor.decompress(buffer, offset + n, length - n)) > 0) {
        n += uncompressedBytes;
      }
    }

    returnByteArray(compressedBuffer);
    returnByteArray(decompressedBuffer);
    returnDecompressor(decompressor);

    return n;
  }

  /** Positioned read fully. It is thread-safe */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkStream();
    if (!(in instanceof PositionedReadable)) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
          + " does not support positioned readFully.");
    }
    if (uncompressedIndexes.get(uncompressedIndexes.size() - 1) < position + length) {
      throw new EOFException("End of file reached before reading fully.");
    }

    if (length > 0) {
      // This operation does not change the current offset of the file
      read(position, buffer, offset, length);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  /** Seek to an uncompressed position. */
  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    checkStream();
    /*
     * If data of target pos in the underlying stream has already been read
     * and decompressed in outBuffer, we just need to re-position outBuffer.
     */
    if (pos <= currentUncompressedIndex && pos >= (currentUncompressedIndex - outBuffer.remaining())) {
      int forward = (int) (pos - (currentUncompressedIndex - outBuffer.remaining()));
      if (forward > 0) {
        outBuffer.position(outBuffer.position() + forward);
      }
    } else {
      if (!(in instanceof Seekable)) {
        throw new UnsupportedOperationException(in.getClass().getCanonicalName()
            + " does not support seek.");
      }
//      ((Seekable) in).seek(pos);
      resetStreamOffset(pos);
    }
  }

  /** Skip n bytes */
  @Override
  public long skip(long n) throws IOException {
    Preconditions.checkArgument(n >= 0, "Negative skip length.");
    checkStream();

    if (n == 0) {
      return 0;
    } else if (n <= outBuffer.remaining()) {
      int pos = outBuffer.position() + (int) n;
      outBuffer.position(pos);
      return n;
    } else {
      /*
       * Subtract outBuffer.remaining() to see how many bytes we need to
       * skip in the underlying stream. Add outBuffer.remaining() to the
       * actual number of skipped bytes in the underlying stream to get the
       * number of skipped bytes from the user's point of view.
       */
      n -= outBuffer.remaining();
      final long currentStreamOffset = currentUncompressedIndex - outBuffer.remaining();
      resetStreamOffset(currentUncompressedIndex + n);
      return currentUncompressedIndex - outBuffer.remaining() - currentStreamOffset;
    }
  }

  /** Get underlying stream position. */
  @Override
  public long getPos() throws IOException {
    checkStream();
    // Equals: ((Seekable) in).getPos() - outBuffer.remaining()
    return currentUncompressedIndex - outBuffer.remaining();
  }

  @Override
  public int available() throws IOException {
    checkStream();

    return outBuffer.remaining();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
  }

  @Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    Preconditions.checkArgument(targetPos >= 0,
        "Cannot seek to negative offset.");
    checkStream();
    if (!(in instanceof Seekable)) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
          + " does not support seekToNewSource.");
    }
    boolean result = ((Seekable) in).seekToNewSource(getCompressedIndexBefore(targetPos));
    resetStreamOffset(targetPos);
    return result;
  }

  @Override
  public void setReadahead(Long readahead) throws IOException,
      UnsupportedOperationException {
    if (!(in instanceof CanSetReadahead)) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
          + " does not support setting the readahead caching strategy.");
    }
    ((CanSetReadahead) in).setReadahead(readahead);
  }

  @Override
  public void setDropBehind(Boolean dropCache) throws IOException,
      UnsupportedOperationException {
    if (!(in instanceof CanSetReadahead)) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
          + " stream does not support setting the drop-behind caching"
          + " setting.");
    }
    ((CanSetDropBehind) in).setDropBehind(dropCache);
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    if (in instanceof HasFileDescriptor) {
      return ((HasFileDescriptor) in).getFileDescriptor();
    } else if (in instanceof FileInputStream) {
      return ((FileInputStream) in).getFD();
    } else {
      return null;
    }
  }

  @Override
  public int read() throws IOException {
    return (read(oneByteBuf, 0, 1) == -1) ? -1 : (oneByteBuf[0] & 0xff);
  }

  private void checkStream() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }

  /**
   * Get byte array from pool
   */
  private byte[] getByteArray() {
    byte[] byteArray = byteArrayPool.poll();
    if (byteArray == null) {
      byteArray = new byte[bufferSize];
    }

    return byteArray;
  }

  /**
   * Return byte array to pool
   */
  private void returnByteArray(byte[] byteArray) {
    if (byteArray != null) {
      byteArrayPool.add(byteArray);
    }
  }

  /** Get decompressor from pool */
  private Decompressor getDecompressor() {
    Decompressor decompressor = decompressorPool.poll();
    if (decompressor == null) {
      if (codec.getDecompressorType() == ZlibDecompressor.class) {
        // native ZlibDecompressor doesn't support setting the buffer using Configuration
        decompressor = new ZlibDecompressor(ZlibDecompressor.CompressionHeader.DEFAULT_HEADER, bufferSize + 8192);
      } else {
        decompressor = codec.createDecompressor();
      }
    }

    return decompressor;
  }

  /** Return decompressor to pool */
  private void returnDecompressor(Decompressor decompressor) {
    if (decompressor != null) {
      decompressorPool.add(decompressor);
    }
  }

  /** Clean Byte Array pool */
  private void cleanByteArrayPool() {
    byteArrayPool.clear();
  }

  private void cleanDecompressorPool() {
    decompressorPool.clear();
  }

  @Override
  public void unbuffer() {
    cleanByteArrayPool();
    cleanDecompressorPool();
    StreamCapabilitiesPolicy.unbuffer(in);
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (StringUtils.toLowerCase(capability)) {
      case StreamCapabilities.UNBUFFER:
        return true;
      case StreamCapabilities.READAHEAD:
      case StreamCapabilities.DROPBEHIND:
      case StreamCapabilities.READBYTEBUFFER:
      case StreamCapabilities.PREADBYTEBUFFER:
        if (!(in instanceof StreamCapabilities)) {
          throw new UnsupportedOperationException(in.getClass().getCanonicalName()
              + " does not expose its stream capabilities.");
        }
        return ((StreamCapabilities) in).hasCapability(capability);
      case StreamCapabilities.IOSTATISTICS:
        return (in instanceof StreamCapabilities)
            && ((StreamCapabilities) in).hasCapability(capability);
      default:
        return false;
    }
  }

  @Override
  public IOStatistics getIOStatistics() {
    return retrieveIOStatistics(in);
  }
}
