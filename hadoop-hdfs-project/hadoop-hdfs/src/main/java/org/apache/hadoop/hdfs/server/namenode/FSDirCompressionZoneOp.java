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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.FileCompressionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.CompressionZone;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.COMPRESS_XATTR_FILE_COMPRESSION_INFO;

/**
 * Helper class to perform compression zone operation.
 */
final class FSDirCompressionZoneOp {

  /**
   * Private constructor for preventing FSDirCompressionZoneOp object creation.
   * Static-only class.
   */
  private FSDirCompressionZoneOp() {
  }

  /**
   * Create a compression zone on directory path using the specified key.
   *
   * @param fsd           fsdirectory
   * @param srcArg        the path of a directory which will be the root of the
   *                      compression zone. The directory must be empty
   * @param pc            permission checker to check fs permission
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @return FileStatus
   * @throws IOException
   */
  static FileStatus createCompressionZone(final FSDirectory fsd,
                                          final String srcArg, final FSPermissionChecker pc, final String codec, final boolean logRetryCache) throws IOException {

    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);

    final INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, srcArg, DirOp.WRITE);
      final XAttr czXAttr = fsd.czManager.createCompressionZone(iip, codec);
      xAttrs.add(czXAttr);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetXAttrs(iip.getPath(), xAttrs, logRetryCache);
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * Get the compression zone for the specified path.
   *
   * @param fsd    fsdirectory
   * @param srcArg the path of a file or directory to get the CZ for
   * @param pc     permission checker to check fs permission
   * @return the CZ with file status.
   */
  static Map.Entry<CompressionZone, FileStatus> getCZForPath(
      final FSDirectory fsd, final String srcArg, final FSPermissionChecker pc)
      throws IOException {
    final INodesInPath iip;
    final CompressionZone ret;
    fsd.readLock();
    try {
      iip = fsd.resolvePath(pc, srcArg, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      ret = fsd.czManager.getCZINodeForPath(iip);
    } finally {
      fsd.readUnlock();
    }
    FileStatus auditStat = fsd.getAuditFileInfo(iip);
    return new AbstractMap.SimpleImmutableEntry<>(ret, auditStat);
  }

  static CompressionZone getCZForPath(final FSDirectory fsd,
                                      final INodesInPath iip) throws IOException {
    fsd.readLock();
    try {
      return fsd.czManager.getCZINodeForPath(iip);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Set the FileCompressionInfo for an INode.
   *
   * @param fsd  fsdirectory
   * @param info file compression information
   * @param flag action when setting xattr. Either CREATE or REPLACE.
   * @throws IOException
   */
  static List<XAttr> setFileCompressionInfo(final FSDirectory fsd,
                                            final INodesInPath iip, final FileCompressionInfo info,
                                            final XAttrSetFlag flag) throws IOException {
    // Make the PB for the xattr
    final HdfsProtos.PerFileCompressionInfoProto proto =
        PBHelperClient.convertPerFileComInfo(info);
    final byte[] protoBytes = proto.toByteArray();
    final XAttr fileCompressionAttr =
        XAttrHelper.buildXAttr(COMPRESS_XATTR_FILE_COMPRESSION_INFO, protoBytes);
    final List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(fileCompressionAttr);
    fsd.writeLock();
    try {
      FSDirXAttrOp.unprotectedSetXAttrs(fsd, iip, xAttrs, EnumSet.of(flag));
    } finally {
      fsd.writeUnlock();
    }
    return xAttrs;
  }

  /**
   * This function combines the per-file compression info (obtained
   * from the inode's XAttrs), and the compression info from its zone, and
   * returns a consolidated FileCompressionInfo instance. Null is returned
   * for non-encrypted or raw files.
   *
   * @param fsd fsdirectory
   * @param iip inodes in the path containing the file, passed in to
   *            avoid obtaining the list of inodes again
   * @return consolidated file compression info; null for non-encrypted files
   */
  static FileCompressionInfo getFileCompressionInfo(final FSDirectory fsd,
                                                    final INodesInPath iip) throws IOException {
    if (iip.isRaw() || !iip.getLastINode().isFile()) {
      return null;
    }
    fsd.readLock();
    try {
      XAttr fileXAttr = FSDirXAttrOp.unprotectedGetXAttrByPrefixedName(
          iip.getLastINode(), iip.getPathSnapshotId(),
          COMPRESS_XATTR_FILE_COMPRESSION_INFO);
      if (fileXAttr == null) {
        NameNode.LOG.warn("Could not find compression XAttr for file " +
            iip.getPath());
        return null;
      }

      try {
        HdfsProtos.PerFileCompressionInfoProto fileProto =
            HdfsProtos.PerFileCompressionInfoProto.parseFrom(
                fileXAttr.getValue());
        return PBHelperClient.convert(fileProto);
      } catch (InvalidProtocolBufferException e) {
        throw new IOException("Could not parse file compression info for " +
            "inode " + iip.getPath(), e);
      }
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Used when creating a new file entry in the namespace.
   *
   * @param dir fsdirectory
   * @param iip inodes in the file path
   * @return FileCompressionInfo for the file
   * @throws RetryStartFileException if key is inconsistent with current zone
   */
  static FileCompressionInfo getFileCompressionInfo(FSDirectory dir,
                                                    INodesInPath iip, String compressionCodec)
      throws IOException {
    FileCompressionInfo fcInfo = null;
    final CompressionZone zone = getCZForPath(dir, iip);
    if (zone != null) {
      // The path is now within an CZ, but we're missing compression parameters
      if (compressionCodec == null) {
        throw new RetryStartFileException();
      }
      // Path is within an CZ and we have provided compression parameters.
      fcInfo = new FileCompressionInfo(compressionCodec);
    }
    return fcInfo;
  }

  static boolean isInAnCZ(final FSDirectory fsd, final INodesInPath iip)
      throws
      IOException {
    if (!fsd.czManager.hasCreatedCompressionZone()) {
      return false;
    }
    fsd.readLock();
    try {
      return fsd.czManager.isInAnCZ(iip);
    } finally {
      fsd.readUnlock();
    }
  }

  static BatchedListEntries<CompressionZone> listCompressionZones(
      final FSDirectory fsd, final long prevId) throws IOException {
    fsd.readLock();
    try {
      return fsd.czManager.listCompressionZones(prevId);
    } finally {
      fsd.readUnlock();
    }
  }

}
