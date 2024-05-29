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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
// import org.apache.hadoop.io.compress.Compressor;
// import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CompressOutputStream extends FilterOutputStream implements
        Syncable, CanSetDropBehind, StreamCapabilities, IOStatisticsSource {
    private final byte[] oneByteBuf = new byte[1];
    private final CompressionCodec codec;
    // private final Compressor compressor;
    private final CompressionOutputStream compressor;
    private final int bufferSize = 512;

    /**
     * Input data buffer. The data starts at inBuffer.position() and ends at
     * inBuffer.limit().
     */
    // private ByteBuffer inBuffer;

    /**
     * Compressed data buffer. The data starts at outBuffer.position() and ends at
     * outBuffer.limit();
     */
    // private ByteBuffer outBuffer;
    // private long streamOffset = 0; // Underlying stream offset.

    private boolean closed;
    private boolean closeOutputStream;

    public CompressOutputStream(OutputStream out, CompressionCodec codec) throws IOException {
        this(out, codec, 0);
    }

    public CompressOutputStream(OutputStream out, CompressionCodec codec, long streamOffset) throws IOException {
        this(out, codec, streamOffset, true);
    }

    public CompressOutputStream(OutputStream out, CompressionCodec codec, long streamOffset, boolean closeOutputStream) throws IOException {
        super(out);
//        this.bufferSize = CryptoStreamUtils.checkBufferSize(codec, bufferSize);
        this.codec = codec;
        // inBuffer = ByteBuffer.allocateDirect(this.bufferSize);
        // outBuffer = ByteBuffer.allocateDirect(this.bufferSize);
        // this.streamOffset = streamOffset;
        this.closeOutputStream = closeOutputStream;
        try {
            compressor = codec.createOutputStream(out);
        } catch (IOException e) {
            throw new IOException(e);
        }
//        compressor = codec.createCompressor();
//        updateCompressor();
    }

    public OutputStream getWrappedStream() {
        return out;
    }

    /**
     * @param b the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException raised on errors performing I/O.
     */
    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        checkStream();
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || off > b.length ||
                len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        compressor.write(b, off, len);
        compressor.flush();

        // writeIndex();
    }

    private byte[] tmpBuf;
    private byte[] getTmpBuf() {
        if (tmpBuf == null) {
            tmpBuf = new byte[bufferSize];
        }
        return tmpBuf;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            try {
                flush();
            } finally {
                if (closeOutputStream) {
                    compressor.close();
//                    compressor.finish();
                    super.close();
                }
//                freeBuffers();
            }
        } finally {
            closed = true;
        }
    }

    /**
     * To flush, we need to encrypt the data in the buffer and write to the
     * underlying stream, then do the flush.
     */
    @Override
    public synchronized void flush() throws IOException {
        if (closed) {
            return;
        }
        compressor.flush();
        super.flush();
    }

    @Override
    public void write(int b) throws IOException {
        oneByteBuf[0] = (byte)(b & 0xff);
        write(oneByteBuf, 0, oneByteBuf.length);
    }

    private void checkStream() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    @Override
    public void setDropBehind(Boolean dropCache) throws IOException,
            UnsupportedOperationException {
        try {
            ((CanSetDropBehind) out).setDropBehind(dropCache);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This stream does not " +
                    "support setting the drop-behind caching.");
        }
    }

    @Override
    public void hflush() throws IOException {
        flush();
        if (out instanceof Syncable) {
            ((Syncable)out).hflush();
        }
    }

    @Override
    public void hsync() throws IOException {
        flush();
        if (out instanceof Syncable) {
            ((Syncable)out).hsync();
        }
    }

//    /** Forcibly free the direct buffers. */
//    private void freeBuffers() {
//        CryptoStreamUtils.freeDB(inBuffer);
//        CryptoStreamUtils.freeDB(outBuffer);
//    }

    @Override
    public boolean hasCapability(String capability) {
        return StoreImplementationUtils.hasCapability(out, capability);
    }

    @Override
    public IOStatistics getIOStatistics() {
        return retrieveIOStatistics(out);
    }
}
