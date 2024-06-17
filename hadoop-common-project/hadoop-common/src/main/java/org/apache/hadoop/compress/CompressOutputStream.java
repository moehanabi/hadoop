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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CompressOutputStream extends FilterOutputStream implements
        Syncable, CanSetDropBehind, StreamCapabilities, IOStatisticsSource {
    private final byte[] oneByteBuf = new byte[1];
    private final CompressionCodec codec;
    private final Compressor compressor;
//    private final int compressSize;
    private ByteBuffer uncompressedDirectBuf = null;
    private byte[] uncompressedBuf;
    private byte[] compressedBuf;
//    private int uncompressedBufOff = 0;
//    private int uncompressedBufLen = 0;
    private final String filePath;
    private long currentUncompressedIndex = 0;
    private long currentCompressedIndex = 0;
    private long nextUncompressedIndex = 0;
    private long nextCompressedIndex = 0;
    private final ArrayList<Long> uncompressedIndexes = new ArrayList<>();
    private final ArrayList<Long> compressedIndexes = new ArrayList<>();
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

    public CompressOutputStream(OutputStream out, CompressionCodec codec, int compressSize, String filePath) throws IOException {
        this(out, codec, 0, compressSize, filePath);
    }

    public CompressOutputStream(OutputStream out, CompressionCodec codec, long streamOffset, int compressSize, String filePath) throws IOException {
        this(out, codec, streamOffset, compressSize, true, filePath);
    }

    public CompressOutputStream(OutputStream out, CompressionCodec codec, long streamOffset, int compressSize, boolean closeOutputStream, String filePath) throws IOException {
        super(out);

        if (out == null || codec == null) {
            throw new NullPointerException();
        } else if (compressSize <= 0) {
            throw new IllegalArgumentException("Illegal compressSize");
        }

        this.codec = codec;
        this.filePath = filePath;
        System.out.println("FilePath:" + filePath);
//        this.compressSize = compressSize;
        uncompressedDirectBuf = ByteBuffer.allocateDirect(compressSize);
        uncompressedBuf = new byte[compressSize];
        compressedBuf = new byte[compressSize];
        compressor = codec.createCompressor();

        // this.streamOffset = streamOffset;
        this.closeOutputStream = closeOutputStream;
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
            final int uncompressedBufLen = uncompressedDirectBuf.limit();
            currentUncompressedIndex += uncompressedBufLen;

            uncompressedDirectBuf.get(uncompressedBuf);
            compress(uncompressedBuf, 0, uncompressedBufLen);
            uncompressedDirectBuf.clear();
        }
        uncompressedDirectBuf.put(b, off, len);
    }

    private void addIndex() {
        uncompressedIndexes.add(currentUncompressedIndex);
        compressedIndexes.add(currentCompressedIndex);
    }

    private void writeIndex() throws IOException {
        for(int i = 0; i < uncompressedIndexes.size(); i++) {
            System.out.println("Uncompressed Index: " + uncompressedIndexes.get(i) + " Compressed Index: " + compressedIndexes.get(i));
        }

        ByteArrayOutputStream uncompressedIndexBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream compressedIndexBytes = new ByteArrayOutputStream();
        ObjectOutputStream uncompressedIndexObj = new ObjectOutputStream(uncompressedIndexBytes);
        ObjectOutputStream compressedIndexObj = new ObjectOutputStream(compressedIndexBytes);
        uncompressedIndexObj.writeObject(uncompressedIndexes);
        compressedIndexObj.writeObject(compressedIndexes);
        setXAttrForFile(filePath, "user.uncompressedIndex", uncompressedIndexBytes.toByteArray());
        setXAttrForFile(filePath, "user.compressedIndex", compressedIndexBytes.toByteArray());
    }

    private void setXAttrForFile(String filePath, String xattrName, byte[] xattrValue) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);

        // Check if the file exists
        if (!fs.exists(path)) {
            throw new IOException("File does not exist: " + filePath);
        }

        // Check if the current user has the permission to set xattr
        FsPermission permission = fs.getFileStatus(path).getPermission();
        if (!permission.getUserAction().implies(FsAction.WRITE)) {
            throw new IOException("The current user does not have the permission to set xattr for file: " + filePath);
        }

        // Set xattr for the file
        fs.setXAttr(path, xattrName, xattrValue);
    }

    public void compress(byte[] b, int off, int len) throws IOException {
        // Sanity checks
        if (compressor.finished()) {
            throw new IOException("write beyond end of stream");
        }
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        compressor.setInput(b, off, len);
        int compressedLen = 0;
        while ((compressedLen = compressor.compress(compressedBuf, 0, compressedBuf.length)) > 0) {
            out.write(compressedBuf, 0, compressedLen);
            currentCompressedIndex += compressedLen;
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
        if (uncompressedDirectBuf.remaining() > 0) {
            addIndex();
            uncompressedDirectBuf.flip();
            final int uncompressedBufLen = uncompressedDirectBuf.limit();
            currentUncompressedIndex += uncompressedBufLen;

            uncompressedDirectBuf.get(uncompressedBuf);
            compress(uncompressedBuf, 0, uncompressedBufLen);
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
        writeIndex();
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
