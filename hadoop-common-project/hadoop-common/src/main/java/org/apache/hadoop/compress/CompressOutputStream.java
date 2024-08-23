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
import org.apache.hadoop.io.compress.Compressor;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CompressOutputStream extends FilterOutputStream implements
        Syncable, CanSetDropBehind, StreamCapabilities, IOStatisticsSource {
    private final byte[] oneByteBuf = new byte[1];
    private final Compressor compressor;
//    private final int compressSize;
    private ByteBuffer uncompressedDirectBuf = null;
    private byte[] uncompressedBuf;
    private byte[] compressedBuf;
//    private int uncompressedBufOff = 0;
//    private int uncompressedBufLen = 0;
    private double compressionRatio;

    // for xattr:
    CompressIndexWriter indexWriter;
    private long currentUncompressedIndex = 0;
    private long currentCompressedIndex = 0;
    private long nextUncompressedIndex = 0;
    private long nextCompressedIndex = 0;
    private ArrayList<Long> uncompressedIndexes;
    private ArrayList<Long> compressedIndexes;
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

    public CompressOutputStream(OutputStream out, Compressor compressor, long streamOffset, int compressSize, CompressIndexWriter compressIndexWriter, ArrayList<Long> uncompressedIndexes, ArrayList<Long> compressedIndexes, double compressionRatio) throws IOException {
        this(out, compressor, streamOffset, compressSize, true, compressIndexWriter, uncompressedIndexes, compressedIndexes, compressionRatio);
    }

    public CompressOutputStream(OutputStream out, Compressor compressor, long streamOffset, int compressSize, boolean closeOutputStream, CompressIndexWriter compressIndexWriter, ArrayList<Long> uncompressedIndexes, ArrayList<Long> compressedIndexes, double compressionRatio) throws IOException {
        super(out);

        if (out == null || compressor == null || compressIndexWriter == null) {
            throw new NullPointerException();
        } else if (compressSize <= 0) {
            throw new IllegalArgumentException("Illegal compressSize");
        }

        this.compressor = compressor;
        this.compressionRatio = compressionRatio;
//        this.compressSize = compressSize;
        uncompressedDirectBuf = ByteBuffer.allocateDirect(compressSize);
        uncompressedBuf = new byte[compressSize];
        compressedBuf = new byte[compressSize + 8192];

        // this.streamOffset = streamOffset;
        this.closeOutputStream = closeOutputStream;

        // for xattr:
        this.indexWriter = compressIndexWriter;

        // for append (decide not use streamOffset temporarily):
        this.uncompressedIndexes = uncompressedIndexes;
        this.compressedIndexes = compressedIndexes;
        if(this.uncompressedIndexes.size() > 0){
            currentUncompressedIndex = this.uncompressedIndexes.get(this.uncompressedIndexes.size()-1);
            currentCompressedIndex = this.compressedIndexes.get(this.compressedIndexes.size()-1);
        }
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
        } else if (len == 0) {
            return;
        }

        if (uncompressedDirectBuf.remaining() < len) {
            addIndex();
            uncompressedDirectBuf.flip();
            final int uncompressedBufLen = uncompressedDirectBuf.remaining();
            currentUncompressedIndex += uncompressedBufLen;

            uncompressedDirectBuf.get(uncompressedBuf, 0, uncompressedBufLen);
            compress(uncompressedBuf, 0, uncompressedBufLen);
            uncompressedDirectBuf.clear();
        }
        uncompressedDirectBuf.put(b, off, len);
    }

    private void addIndex() {
        uncompressedIndexes.add(currentUncompressedIndex);
        compressedIndexes.add(currentCompressedIndex);
    }

    public void compress(byte[] b, int off, int len) throws IOException {
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        compressor.reset();
        compressor.setInput(b, off, len);
        compressor.finish();

        int compressedLen;
        int totalCompressedLen = 0;
        try {
            while ((compressedLen = compressor.compress(compressedBuf, totalCompressedLen, compressedBuf.length - totalCompressedLen)) > 0) {
                totalCompressedLen += compressedLen;
            }
            if (totalCompressedLen < len * compressionRatio) {
                out.write(compressedBuf, 0, totalCompressedLen);
                currentCompressedIndex += totalCompressedLen;
            } else {
                // If the compressed data is too large, just write the original data
                out.write(b, off, len);
                currentCompressedIndex += len;
            }
        } catch (Exception e) {
            out.write(b, off, len);
            currentCompressedIndex += len;
        }
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
                    super.close();
                    compressor.end();
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
        uncompressedDirectBuf.flip();
        if (uncompressedDirectBuf.remaining() > 0) {
            addIndex();
            final int uncompressedBufLen = uncompressedDirectBuf.remaining();
            currentUncompressedIndex += uncompressedBufLen;

            uncompressedDirectBuf.get(uncompressedBuf, 0, uncompressedBufLen);
            compress(uncompressedBuf, 0, uncompressedBufLen);
            uncompressedDirectBuf.clear();
        }
//        compressor.finish();
//        while (!compressor.finished()) {
//            int compressedLen = 0;
//            while ((compressedLen = compressor.compress(compressedBuf, 0, compressedBuf.length)) > 0) {
//                out.write(compressedBuf, 0, compressedLen);
//                currentCompressedIndex += compressedLen;
//            }
//        }
        addIndex();
        indexWriter.writeIndex(uncompressedIndexes, compressedIndexes);
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
