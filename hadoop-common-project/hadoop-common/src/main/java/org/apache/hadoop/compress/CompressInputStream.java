/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.compress;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.eclipse.jetty.util.log.Log;

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

// implements
//        Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor,
//        CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess,
//        ReadableByteChannel, CanUnbuffer, StreamCapabilities,
//        ByteBufferPositionedReadable, IOStatisticsSource
public class CompressInputStream extends FilterInputStream implements Seekable, PositionedReadable {
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
  /**
   * Input data buffer. The data starts at inBuffer.position() and ends at
   * to inBuffer.limit().
   */
  private ByteBuffer inBuffer;
  private byte[] inBufferArray;
  /**
   * The decompressed data buffer. The data starts at outBuffer.position() and
   * ends at outBuffer.limit();
   */
  private ByteBuffer outBuffer;
  private byte[] outBufferArray;
  private long streamOffset = 0; // Underlying stream offset.
  /**
   * Whether the underlying stream supports
   * {@link org.apache.hadoop.fs.ByteBufferReadable}
   */
  private Boolean usingByteBufferRead = null;

  // /** DirectBuffer pool */
  // private final Queue<ByteBuffer> bufferPool =
  //         new ConcurrentLinkedQueue<ByteBuffer>();
  //  /**
//   * Padding = pos%(algorithm blocksize); Padding is put into {@link #inBuffer}
//   * before any other data goes in. The purpose of padding is to put the input
//   * data at proper position.
//   */
//  private byte padding;
  private boolean closed;
  private ArrayList<Long> uncompressedIndexes = new ArrayList<>();
  private ArrayList<Long> compressedIndexes = new ArrayList<>();
  private long currentUncompressedIndex = 0;
  private long currentCompressedIndex = 0;

  public CompressInputStream(InputStream in, CompressionCodec codec,
                           int bufferSize, byte[] uncompressedIndexesBytes, byte[] compressedIndexesBytes) throws IOException {
    this(in, codec, bufferSize, 0, uncompressedIndexesBytes, compressedIndexesBytes);
  }

  public CompressInputStream(InputStream in, CompressionCodec codec,
                             int bufferSize, long streamOffset, byte[] uncompressedIndexesBytes, byte[] compressedIndexesBytes) throws IOException {
    super(in);
    this.bufferSize = bufferSize;
    this.codec = codec;
    this.streamOffset = streamOffset;
    isByteBufferReadable = in instanceof ByteBufferReadable;
    isReadableByteChannel = in instanceof ReadableByteChannel;
    inBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    outBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    inBufferArray = new byte[this.bufferSize];
    outBufferArray = new byte[this.bufferSize];
    decompressor = getDecompressor();

    getIndexes(uncompressedIndexesBytes, compressedIndexesBytes);
    resetStreamOffset(streamOffset);
  }

  private void resetStreamOffset(long streamOffset) throws IOException {
    // find out the streamOffset is between which two uncompressedIndexes
    this.streamOffset = streamOffset;

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
      this.streamOffset = currentCompressedIndex;
    }
    outBuffer.position(pos);
//    currentUncompressedIndex = streamOffset;
  }

  private void getIndexes(byte[] uncompressedIndexesBytes, byte[] compressedIndexesBytes) throws IOException {
    // Get uncompressedIndexes and compressedIndexes from xattr
    try {
      uncompressedIndexes = (ArrayList<Long>) new ObjectInputStream(new ByteArrayInputStream(uncompressedIndexesBytes)).readObject();
      compressedIndexes = (ArrayList<Long>) new ObjectInputStream(new ByteArrayInputStream(compressedIndexesBytes)).readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Error reading xattr for file");
    }
  }

  public InputStream getWrappedStream() {
    return in;
  }

  private long getUncompressedIndexBefore(long compressedIndex) {
    int i = 0;
    while (i < compressedIndexes.size() && compressedIndex >= compressedIndexes.get(i)) {
      i++;
    }
    return uncompressedIndexes.get(i - 1);
  }

  private long getCompressedIndexBefore(long uncompressedIndex) {
    int i = 0;
    while (i < uncompressedIndexes.size() && uncompressedIndex >= uncompressedIndexes.get(i)) {
      i++;
    }
    return compressedIndexes.get(i - 1);
  }

  private long getUncompressedIndexAfter(long compressedIndex) {
    int i = 0;
    while (i < compressedIndexes.size() && compressedIndex >= compressedIndexes.get(i)) {
      i++;
    }
    if(i>=uncompressedIndexes.size())
      return -1;
    return uncompressedIndexes.get(i);
  }

  private long getCompressedIndexAfter(long uncompressedIndex) {
    int i = 0;
    while (i < uncompressedIndexes.size() && uncompressedIndex >= uncompressedIndexes.get(i)) {
      i++;
    }
    if(i>=uncompressedIndexes.size())
      return -1;
    return compressedIndexes.get(i);
  }

  /**
   * Decryption is buffer based.
   * If there is data in {@link #outBuffer}, then read it out of this buffer.
   * If there is no data in {@link #outBuffer}, then read more from the
   * underlying stream and do the decompression.
   * @param b the buffer into which the decompressed data is read.
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
      int n = 0;
      if ((n = readAndDecompress()) <= 0) {
        return n;
      }
//      padding = afterDecryption(decompressor, inBuffer, streamOffset, iv);
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
          while (n>=0 && inBuffer.hasRemaining()) {
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
        n = readFromUnderlyingStream(inBuffer, toRead);
      }
    } else {
      if (usingByteBufferRead) {
        while (n>=0 && inBuffer.hasRemaining()) {
          n += isByteBufferReadable ? ((ByteBufferReadable) in).read(inBuffer) :
                  ((ReadableByteChannel) in).read(inBuffer);
        }
      } else {
        n = readFromUnderlyingStream(inBuffer, toRead);
      }
    }
    if (n <= 0) {
      return n;
    }

    streamOffset += n; // Read n bytes
    decompress(decompressor, inBuffer, outBuffer, (byte) 0);
    return n;
  }

  /** Read data from underlying stream. */
  private int readFromUnderlyingStream(ByteBuffer inBuffer, int toRead) throws IOException {
    final byte[] tmp = getTmpBuf();
    int n = 0;
    while (n>=0 && inBuffer.hasRemaining()) {
      n += in.read(tmp, 0, toRead);
    }
    if (n > 0) {
      inBuffer.put(tmp, 0, n);
    }
    return n;
  }

   private byte[] tmpBuf;
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
                       ByteBuffer outBuffer, byte padding) throws IOException {
//    Preconditions.checkState(inBuffer.position() >= padding);
//    if(inBuffer.position() == padding) {
//      // There is no real data in inBuffer.
//      return;
//    }
    inBuffer.flip();
    outBuffer.clear();
    final int compressedBytes = inBuffer.remaining();
    inBuffer.get(inBufferArray, 0, compressedBytes);
    decompressor.setInput(inBufferArray, 0, compressedBytes);
    final int uncompressedBytes = decompressor.decompress(outBufferArray, 0, bufferSize);
    outBuffer.put(outBufferArray, 0, uncompressedBytes);
    inBuffer.clear();
    outBuffer.flip();
    currentUncompressedIndex += uncompressedBytes;
    currentCompressedIndex += compressedBytes;
//    if (padding > 0) {
//      /*
//       * The plain text and cipher text have a 1:1 mapping, they start at the
//       * same position.
//       */
//      outBuffer.position(padding);
//    }
  }

//  /**
//   * This method is executed immediately after decompression. Check whether
//   * decompressor should be updated and recalculate padding if needed.
//   */
//  private byte afterDecryption(CompressionInputStream decompressor, ByteBuffer inBuffer,
//                               long position, byte[] iv) throws IOException {
//    byte padding = 0;
//    if (decompressor.isContextReset()) {
//      /*
//       * This code is generally not executed since the decompressor usually
//       * maintains decompression context (e.g. the counter) internally. However,
//       * some implementations can't maintain context so a re-init is necessary
//       * after each decompression call.
//       */
//      updateCompressionInputStream(decompressor, position, iv);
//      padding = getPadding(position);
//      inBuffer.position(padding);
//    }
//    return padding;
//  }
//
//  private long getCounter(long position) {
//    return position / codec.getCipherSuite().getAlgorithmBlockSize();
//  }
//
//  private byte getPadding(long position) {
//    return (byte)(position % codec.getCipherSuite().getAlgorithmBlockSize());
//  }

//  /** Calculate the counter and iv, update the decompressor. */
//  private void updateCompressionInputStream(CompressionInputStream decompressor, long position, byte[] iv)
//          throws IOException {
//    final long counter = getCounter(position);
//    codec.calculateIV(initIV, counter, iv);
//    decompressor.init(key, iv);
//  }

//  /**
//   * Reset the underlying stream offset; clear {@link #inBuffer} and
//   * {@link #outBuffer}. This Typically happens during {@link #seek(long)}
//   * or {@link #skip(long)}.
//   */
//  private void resetStreamOffset(long offset) throws IOException {
//    streamOffset = offset;
//    inBuffer.clear();
//    outBuffer.clear();
//    outBuffer.limit(0);
//    updateCompressionInputStream(decompressor, offset, iv);
//    padding = getPadding(offset);
//    inBuffer.position(padding); // Set proper position for input data.
//  }

  //@Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;
  }

  /** Positioned read. It is thread-safe */
  //@Override
  public int read(long position, byte[] buffer, int offset, int length)
          throws IOException {
    checkStream();
    if (!(in instanceof PositionedReadable)) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
              + " does not support positioned read.");
    }
    final int n = ((PositionedReadable) in).read(position, buffer, offset,
            length);
    if (n > 0) {
      // This operation does not change the current offset of the file
      read(buffer, offset, n);
    }

    return n;
  }

  /**
   * Positioned read using {@link ByteBuffer}s. This method is thread-safe.
   */
//  //@Override
//  public int read(long position, final ByteBuffer buf)
//          throws IOException {
//    checkStream();
//    if (!(in instanceof ByteBufferPositionedReadable)) {
//      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
//              + " does not support positioned reads with byte buffers.");
//    }
//    int bufPos = buf.position();
//    final int n = ((ByteBufferPositionedReadable) in).read(position, buf);
//    if (n > 0) {
//      // This operation does not change the current offset of the file
//      decompress(position, buf, n, bufPos);
//    }
//
//    return n;
//  }

//  /**
//   * Positioned readFully using {@link ByteBuffer}s. This method is thread-safe.
//   */
//  //@Override
//  public void readFully(long position, final ByteBuffer buf)
//          throws IOException {
//    checkStream();
//    if (!(in instanceof ByteBufferPositionedReadable)) {
//      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
//              + " does not support positioned reads with byte buffers.");
//    }
//    int bufPos = buf.position();
//    ((ByteBufferPositionedReadable) in).readFully(position, buf);
//    final int n = buf.position() - bufPos;
//    if (n > 0) {
//      // This operation does not change the current offset of the file
//      decompress(position, buf, n, bufPos);
//    }
//  }

//  /**
//   * Decrypt length bytes in buffer starting at offset. Output is also put
//   * into buffer starting at offset. It is thread-safe.
//   */
//  private void decompress(long position, byte[] buffer, int offset, int length)
//          throws IOException {
//    ByteBuffer localInBuffer = null;
//    ByteBuffer localOutBuffer = null;
//    CompressionInputStream decompressor = null;
//    try {
////      localInBuffer = getBuffer();
////      localOutBuffer = getBuffer();
//      decompressor = getCompressionInputStream();
////      updateCompressionInputStream(decompressor, position, iv);
////      byte padding = getPadding(position);
////      localInBuffer.position(padding); // Set proper position for input data.
//
//      int n = 0;
//      while (n < length) {
//        int toDecrypt = Math.min(length - n, localInBuffer.remaining());
////        localInBuffer.put(buffer, offset + n, toDecrypt);
//        // Do decompression
//        decompressor.read(buffer, offset + n, toDecrypt);
//
////        localOutBuffer.get(buffer, offset + n, toDecrypt);
//        n += toDecrypt;
////        padding = afterDecryption(decompressor, localInBuffer, position + n, iv);
//      }
//    } finally {
//      returnBuffer(localInBuffer);
//      returnBuffer(localOutBuffer);
//      returnCompressionInputStream(decompressor);
//    }
//  }

  /**
   * Decrypts the given {@link ByteBuffer} in place. {@code length} bytes are
   * decompressed from {@code buf} starting at {@code start}.
   * {@code buf.position()} and {@code buf.limit()} are unchanged after this
   * method returns. This method is thread-safe.
   *
   * <p>
   *   This method decompresss the input buf chunk-by-chunk and writes the
   *   decompressed output back into the input buf. It uses two local buffers
   *   taken from the {@link #bufferPool} to assist in this process: one is
   *   designated as the input buffer and it stores a single chunk of the
   *   given buf, the other is designated as the output buffer, which stores
   *   the output of decompressing the input buffer. Both buffers are of size
   *   {@link #bufferSize}.
   * </p>
   *
   * <p>
   *   Decryption is done by using a {@link CompressionInputStream} and the
   *   {@link #decompress(CompressionInputStream, ByteBuffer, ByteBuffer, byte)} method. Once
   *   the decompressed data is written into the output buffer, is is copied back
   *   into buf. Both buffers are returned back into the pool once the entire
   *   buf is decompressed.
   * </p>
   *
   * @param filePosition the current position of the file being read
   * @param buf the {@link ByteBuffer} to decompress
   * @param length the number of bytes in {@code buf} to decompress
   * @param start the position in {@code buf} to start decompressing data from
   */
//  private void decompress(long filePosition, ByteBuffer buf, int length, int start)
//          throws IOException {
//    ByteBuffer localInBuffer = null;
//    ByteBuffer localOutBuffer = null;
//
//    // Duplicate the buffer so we don't have to worry about resetting the
//    // original position and limit at the end of the method
//    buf = buf.duplicate();
//
//    int decompressedBytes = 0;
//    CompressionInputStream localCompressionInputStream = null;
//    try {
//      localInBuffer = getBuffer();
//      localOutBuffer = getBuffer();
//      localCompressionInputStream = getCompressionInputStream();
////      updateCompressionInputStream(localCompressionInputStream, filePosition, localIV);
////      byte localPadding = getPadding(filePosition);
//      // Set proper filePosition for inputdata.
//      localInBuffer.position(localPadding);
//
//      while (decompressedBytes < length) {
//        buf.position(start + decompressedBytes);
//        buf.limit(start + decompressedBytes +
//                Math.min(length - decompressedBytes, localInBuffer.remaining()));
//        localInBuffer.put(buf);
//        // Do decompression
//        try {
//          decompress(localCompressionInputStream, localInBuffer, localOutBuffer, localPadding);
//          buf.position(start + decompressedBytes);
//          buf.limit(start + length);
//          decompressedBytes += localOutBuffer.remaining();
//          buf.put(localOutBuffer);
//        } finally {
////          localPadding = afterDecryption(localCompressionInputStream, localInBuffer,
////                  filePosition + length, localIV);
//        }
//      }
//    } finally {
//      returnBuffer(localInBuffer);
//      returnBuffer(localOutBuffer);
//      returnCompressionInputStream(localCompressionInputStream);
//    }
//  }

  /** Positioned read fully. It is thread-safe */
  //@Override
  public void readFully(long position, byte[] buffer, int offset, int length)
          throws IOException {
    checkStream();
    if (!(in instanceof PositionedReadable)) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
              + " does not support positioned readFully.");
    }
    ((PositionedReadable) in).readFully(position, buffer, offset, length);
    if (length > 0) {
      // This operation does not change the current offset of the file
      read(position, buffer, offset, length);
    }
  }

  //@Override
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
    if (pos <= getUncompressedIndexAfter(currentCompressedIndex) && pos >= (getUncompressedIndexAfter(currentCompressedIndex) - outBuffer.remaining())) {
      int forward = (int) (pos - (getUncompressedIndexAfter(currentCompressedIndex) - outBuffer.remaining()));
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
  //@Override
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
      final long currentStreamOffset = streamOffset;
      resetStreamOffset(currentUncompressedIndex + n);
      return streamOffset - currentStreamOffset;
    }
  }

  /** Get underlying stream position. */
  //@Override
  public long getPos() throws IOException {
    checkStream();
    // Equals: ((Seekable) in).getPos() - outBuffer.remaining()
    return streamOffset - outBuffer.remaining();
  }
//
//  /** ByteBuffer read. */
//  //@Override
//  public int read(ByteBuffer buf) throws IOException {
//    checkStream();
//    if (isByteBufferReadable || isReadableByteChannel) {
//      final int unread = outBuffer.remaining();
//      if (unread > 0) { // Have unread decompressed data in buffer.
//        int toRead = buf.remaining();
//        if (toRead <= unread) {
//          final int limit = outBuffer.limit();
//          outBuffer.limit(outBuffer.position() + toRead);
//          buf.put(outBuffer);
//          outBuffer.limit(limit);
//          return toRead;
//        } else {
//          buf.put(outBuffer);
//        }
//      }
//
//      final int pos = buf.position();
//      final int n = isByteBufferReadable ? ((ByteBufferReadable) in).read(buf) :
//              ((ReadableByteChannel) in).read(buf);
//      if (n > 0) {
//        streamOffset += n; // Read n bytes
//        decompress(buf, n, pos);
//      }
//
//      if (n >= 0) {
//        return unread + n;
//      } else {
//        if (unread == 0) {
//          return -1;
//        } else {
//          return unread;
//        }
//      }
//    } else {
//      int n = 0;
//      if (buf.hasArray()) {
//        n = read(buf.array(), buf.position(), buf.remaining());
//        if (n > 0) {
//          buf.position(buf.position() + n);
//        }
//      } else {
//        byte[] tmp = new byte[buf.remaining()];
//        n = read(tmp);
//        if (n > 0) {
//          buf.put(tmp, 0, n);
//        }
//      }
//      return n;
//    }
//  }
//
//  /**
//   * Decrypts the given {@link ByteBuffer} in place. {@code length} bytes are
//   * decompressed from {@code buf} starting at {@code start}.
//   * {@code buf.position()} and {@code buf.limit()} are unchanged after this
//   * method returns.
//   *
//   * @see #decompress(long, ByteBuffer, int, int)
//   */
//  private void decompress(ByteBuffer buf, int length, int start)
//          throws IOException {
//    buf = buf.duplicate();
//    int decompressedBytes = 0;
//    while (decompressedBytes < length) {
//      buf.position(start + decompressedBytes);
//      buf.limit(start + decompressedBytes +
//              Math.min(length - decompressedBytes, inBuffer.remaining()));
//      inBuffer.put(buf);
//      // Do decompression
//      try {
//        decompress(decompressor, inBuffer, outBuffer, padding);
//        buf.position(start + decompressedBytes);
//        buf.limit(start + length);
//        decompressedBytes += outBuffer.remaining();
//        buf.put(outBuffer);
//      } finally {
////        padding = afterDecryption(decompressor, inBuffer,
////                streamOffset - (length - decompressedBytes), iv);
//      }
//    }
//  }

  //@Override
  public int available() throws IOException {
    checkStream();

    return in.available() + outBuffer.remaining();
  }

  //@Override
  public boolean markSupported() {
    return false;
  }

  //@Override
  public void mark(int readLimit) {
  }

  //@Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }

  //@Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    Preconditions.checkArgument(targetPos >= 0,
            "Cannot seek to negative offset.");
    checkStream();
    if (!(in instanceof Seekable)) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
              + " does not support seekToNewSource.");
    }
    boolean result = ((Seekable) in).seekToNewSource(targetPos);
//    resetStreamOffset(targetPos);
    return result;
  }

//  //@Override
//  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength,
//                         EnumSet<ReadOption> opts) throws IOException,
//          UnsupportedOperationException {
//    checkStream();
//    if (outBuffer.remaining() > 0) {
//      if (!(in instanceof Seekable)) {
//        throw new UnsupportedOperationException(in.getClass().getCanonicalName()
//                + " does not support seek.");
//      }
//      // Have some decompressed data unread, need to reset.
//      ((Seekable) in).seek(getPos());
////      resetStreamOffset(getPos());
//    }
//    if (!(in instanceof HasEnhancedByteBufferAccess)) {
//      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
//              + " does not support enhanced byte buffer access.");
//    }
//    final ByteBuffer buffer = ((HasEnhancedByteBufferAccess) in).
//            read(bufferPool, maxLength, opts);
//    if (buffer != null) {
//      final int n = buffer.remaining();
//      if (n > 0) {
//        streamOffset += buffer.remaining(); // Read n bytes
//        final int pos = buffer.position();
//        decompress(buffer, n, pos);
//      }
//    }
//    return buffer;
//  }

//  //@Override
//  public void releaseBuffer(ByteBuffer buffer) {
//    if (!(in instanceof HasEnhancedByteBufferAccess)) {
//      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
//              + " does not support release buffer.");
//    }
//    ((HasEnhancedByteBufferAccess) in).releaseBuffer(buffer);
//  }

//  //@Override
//  public void setReadahead(Long readahead) throws IOException,
//          UnsupportedOperationException {
//    if (!(in instanceof CanSetReadahead)) {
//      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
//              + " does not support setting the readahead caching strategy.");
//    }
//    ((CanSetReadahead) in).setReadahead(readahead);
//  }

//  //@Override
//  public void setDropBehind(Boolean dropCache) throws IOException,
//          UnsupportedOperationException {
//    if (!(in instanceof CanSetReadahead)) {
//      throw new UnsupportedOperationException(in.getClass().getCanonicalName()
//              + " stream does not support setting the drop-behind caching"
//              + " setting.");
//    }
//    ((CanSetDropBehind) in).setDropBehind(dropCache);
//  }

//  //@Override
//  public FileDescriptor getFileDescriptor() throws IOException {
//    if (in instanceof HasFileDescriptor) {
//      return ((HasFileDescriptor) in).getFileDescriptor();
//    } else if (in instanceof FileInputStream) {
//      return ((FileInputStream) in).getFD();
//    } else {
//      return null;
//    }
//  }

  //@Override
  public int read() throws IOException {
    return (read(oneByteBuf, 0, 1) == -1) ? -1 : (oneByteBuf[0] & 0xff);
  }

  private void checkStream() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }

//  /** Get direct buffer from pool */
//  private ByteBuffer getBuffer() {
//    ByteBuffer buffer = bufferPool.poll();
//    if (buffer == null) {
//      buffer = ByteBuffer.allocateDirect(bufferSize);
//    }
//
//    return buffer;
//  }

//  /** Return direct buffer to pool */
//  private void returnBuffer(ByteBuffer buf) {
//    if (buf != null) {
//      buf.clear();
//      bufferPool.add(buf);
//    }
//  }

  /** Get decompressor from pool */
  private Decompressor getDecompressor() {
    Decompressor decompressor = decompressorPool.poll();
    if (decompressor == null) {
        decompressor = codec.createDecompressor();
    }

    return decompressor;
  }

//  /** Return decompressor to pool */
//  private void returnCompressionInputStream(CompressionInputStream decompressor) {
//    if (decompressor != null) {
//      decompressorPool.add(decompressor);
//    }
//  }

//  //@Override
//  public boolean isOpen() {
//    return !closed;
//  }

//  private void cleanCompressionInputStreamPool() {
//    decompressorPool.clear();
//  }

//  //@Override
//  public void unbuffer() {
//    cleanCompressionInputStreamPool();
//    StreamCapabilitiesPolicy.unbuffer(in);
//  }

//  //@Override
//  public boolean hasCapability(String capability) {
//    switch (StringUtils.toLowerCase(capability)) {
//      case StreamCapabilities.UNBUFFER:
//        return true;
//      case StreamCapabilities.READAHEAD:
//      case StreamCapabilities.DROPBEHIND:
//      case StreamCapabilities.READBYTEBUFFER:
//      case StreamCapabilities.PREADBYTEBUFFER:
//        if (!(in instanceof StreamCapabilities)) {
//          throw new UnsupportedOperationException(in.getClass().getCanonicalName()
//                  + " does not expose its stream capabilities.");
//        }
//        return ((StreamCapabilities) in).hasCapability(capability);
//      case StreamCapabilities.IOSTATISTICS:
//        return (in instanceof StreamCapabilities)
//                && ((StreamCapabilities) in).hasCapability(capability);
//      default:
//        return false;
//    }
//  }

//  //@Override
//  public IOStatistics getIOStatistics() {
//    return retrieveIOStatistics(in);
//  }
}
