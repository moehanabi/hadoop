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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.CompressionZone;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.COMPRESS_XATTR_COMPRESSION_ZONE;

/**
 * Manages the list of compression zones in the filesystem.
 * <p>
 * The CompressionZoneManager has its own lock, but relies on the FSDirectory
 * lock being held for many operations. The FSDirectory lock should not be
 * taken if the manager lock is already held.
 */
public class CompressionZoneManager {

  public static final Logger LOG = LoggerFactory.getLogger(CompressionZoneManager
      .class);

  /**
   * CompressionZoneInt is the internal representation of an compression zone. The
   * external representation of an CZ is embodied in an CompressionZone and
   * contains the CZ's pathname.
   */
  private static class CompressionZoneInt {
    private final long inodeId;
    private final String codec;

    CompressionZoneInt(long inodeId, String codec) {
      this.inodeId = inodeId;
      this.codec = codec;
    }

    long getINodeId() {
      return inodeId;
    }

    String getCodec() {
      return codec;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CompressionZoneInt)) {
        return false;
      }

      CompressionZoneInt b = (CompressionZoneInt)o;
      return new EqualsBuilder()
          .append(inodeId, b.getINodeId())
          .append(codec, b.getCodec())
          .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().
          append(inodeId).
          append(codec).
          toHashCode();
    }
  }

  private TreeMap<Long, CompressionZoneInt> compressionZones = null;
  private final FSDirectory dir;
  private final int maxListCompressionZonesResponses;

  FSDirectory getFSDirectory() {
    return dir;
  }

  /**
   * Construct a new CompressionZoneManager.
   *
   * @param dir Enclosing FSDirectory
   */
  public CompressionZoneManager(FSDirectory dir, Configuration conf) {
    this.dir = dir;
    maxListCompressionZonesResponses = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT
    );
    Preconditions.checkArgument(maxListCompressionZonesResponses >= 0,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES + " " +
            "must be a positive integer."
    );
  }

  /**
   * Add a new compression zone.
   * <p>
   * Called while holding the FSDirectory lock.
   *
   * @param inodeId of the compression zone
   */
  void addCompressionZone(Long inodeId, String codec) {
    assert dir.hasWriteLock();
    unprotectedAddCompressionZone(inodeId, codec);
  }

  /**
   * Add a new compression zone.
   * <p>
   * Does not assume that the FSDirectory lock is held.
   *
   * @param inodeId of the compression zone
   */
  void unprotectedAddCompressionZone(Long inodeId,
      String codec) {
    final CompressionZoneInt cz = new CompressionZoneInt(
        inodeId, codec);
    if (compressionZones == null) {
      compressionZones = new TreeMap<>();
    }
    compressionZones.put(inodeId, cz);
  }

  /**
   * Remove an compression zone.
   * <p>
   * Called while holding the FSDirectory lock.
   */
  void removeCompressionZone(Long inodeId) {
    assert dir.hasWriteLock();
    if (hasCreatedCompressionZone()) {
      compressionZones.remove(inodeId);
    }
  }

  /**
   * Returns true if an IIP is within an compression zone.
   * <p>
   * Called while holding the FSDirectory lock.
   */
  boolean isInAnCZ(INodesInPath iip) throws UnresolvedLinkException,
      SnapshotAccessControlException, IOException {
    assert dir.hasReadLock();
    return (getCompressionZoneForPath(iip) != null);
  }

  /**
   * Returns the full path from an INode id.
   * <p>
   * Called while holding the FSDirectory lock.
   */
  String getFullPathName(Long nodeId) {
    assert dir.hasReadLock();
    INode inode = dir.getInode(nodeId);
    if (inode == null) {
      return null;
    }
    return inode.getFullPathName();
  }

  /**
   * Looks up the CompressionZoneInt for a path within an compression zone.
   * Returns null if path is not within an CZ.
   * <p>
   * Called while holding the FSDirectory lock.
   */
  private CompressionZoneInt getCompressionZoneForPath(INodesInPath iip)
      throws  IOException{
    assert dir.hasReadLock();
    Preconditions.checkNotNull(iip);
    if (!hasCreatedCompressionZone()) {
      return null;
    }

    int snapshotID = iip.getPathSnapshotId();
    for (int i = iip.length() - 1; i >= 0; i--) {
      final INode inode = iip.getINode(i);
      if (inode == null || !inode.isDirectory()) {
        //not found or not a directory, compression zone is supported on
        //directory only.
        continue;
      }
      if (snapshotID == Snapshot.CURRENT_STATE_ID) {
        final CompressionZoneInt czi = compressionZones.get(inode.getId());
        if (czi != null) {
          return czi;
        }
      } else {
        XAttr xAttr = FSDirXAttrOp.unprotectedGetXAttrByPrefixedName(
            inode, snapshotID, COMPRESS_XATTR_COMPRESSION_ZONE);
        if (xAttr != null) {
          try {
            final HdfsProtos.ZoneCompressionInfoProto czProto =
                HdfsProtos.ZoneCompressionInfoProto.parseFrom(xAttr.getValue());
            return new CompressionZoneInt(
                inode.getId(), czProto.getCompressionCodec());
          } catch (InvalidProtocolBufferException e) {
            throw new IOException("Could not parse compression zone for inode "
                + iip.getPath(), e);
          }
        }
      }
    }
    return null;
  }

  /**
   * Looks up the nearest ancestor CompressionZoneInt that contains the given
   * path (excluding itself).
   * Returns null if path is not within an CZ, or the path is the root dir '/'
   * <p>
   * Called while holding the FSDirectory lock.
   */
  private CompressionZoneInt getParentCompressionZoneForPath(INodesInPath iip)
      throws  IOException {
    assert dir.hasReadLock();
    Preconditions.checkNotNull(iip);
    INodesInPath parentIIP = iip.getParentINodesInPath();
    return parentIIP == null ? null : getCompressionZoneForPath(parentIIP);
  }

  /**
   * Returns an CompressionZone representing the cz for a given path.
   * Returns an empty marker CompressionZone if path is not in an cz.
   *
   * @param iip The INodesInPath of the path to check
   * @return the CompressionZone representing the cz for the path.
   */
  CompressionZone getCZINodeForPath(INodesInPath iip)
      throws IOException {
    final CompressionZoneInt czi = getCompressionZoneForPath(iip);
    if (czi == null) {
      return null;
    } else {
      return new CompressionZone(czi.getINodeId(),
          getFullPathName(czi.getINodeId()),
          czi.getCodec());
    }
  }

  /**
   * Create a new compression zone.
   * <p>
   * Called while holding the FSDirectory lock.
   */
  XAttr createCompressionZone(INodesInPath srcIIP, String codec)
      throws IOException {
    assert dir.hasWriteLock();

    // Check if src is a valid path for new CZ creation
    if (srcIIP.getLastINode() == null) {
      throw new FileNotFoundException("cannot find " + srcIIP.getPath());
    }

    INode srcINode = srcIIP.getLastINode();
    if (!srcINode.isDirectory()) {
      throw new IOException("Attempt to create an compression zone for a file.");
    }

    if (hasCreatedCompressionZone() && compressionZones.
        get(srcINode.getId()) != null) {
      throw new IOException(
          "Directory " + srcIIP.getPath() + " is already an compression zone.");
    }

    if (dir.isNonEmptyDirectory(srcIIP)) {
      throw new IOException(
          "Attempt to create an compression zone for a non-empty directory.");
    }
//    final HdfsProtos.ZoneCompressionInfoProto proto =
//        PBHelperClient.convert(codec);
//    final XAttr czXAttr = XAttrHelper
//        .buildXAttr(COMPRESS_XATTR_COMPRESSION_ZONE, proto.toByteArray());
    final HdfsProtos.ZoneCompressionInfoProto proto =
            PBHelperClient.convert(codec);
    final XAttr czXAttr = XAttrHelper
            .buildXAttr(COMPRESS_XATTR_COMPRESSION_ZONE, proto.toByteArray());

    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(czXAttr);
    // updating the xattr will call addCompressionZone,
    // done this way to handle edit log loading
    FSDirXAttrOp.unprotectedSetXAttrs(dir, srcIIP, xattrs,
                                      EnumSet.of(XAttrSetFlag.CREATE));
    return czXAttr;
  }

  /**
   * Cursor-based listing of compression zones.
   * <p>
   * Called while holding the FSDirectory lock.
   */
  BatchedListEntries<CompressionZone> listCompressionZones(long prevId)
      throws IOException {
    assert dir.hasReadLock();
    if (!hasCreatedCompressionZone()) {
      return new BatchedListEntries<CompressionZone>(Lists.newArrayList(), false);
    }
    NavigableMap<Long, CompressionZoneInt> tailMap = compressionZones.tailMap
        (prevId, false);
    final int numResponses = Math.min(maxListCompressionZonesResponses,
        tailMap.size());
    final List<CompressionZone> zones =
        Lists.newArrayListWithExpectedSize(numResponses);

    int count = 0;
    for (CompressionZoneInt czi : tailMap.values()) {
      /*
       Skip CZs that are only present in snapshots. Re-resolve the path to 
       see if the path's current inode ID matches CZ map's INode ID.

       INode#getFullPathName simply calls getParent recursively, so will return
       the INode's parents at the time it was snapshotted. It will not
       contain a reference INode.
      */
      final String pathName = getFullPathName(czi.getINodeId());
      if (!pathResolvesToId(czi.getINodeId(), pathName)) {
        continue;
      }
      // Add the CZ to the result list
      zones.add(new CompressionZone(czi.getINodeId(), pathName,
          czi.getCodec()));
      count++;
      if (count >= numResponses) {
        break;
      }
    }
    final boolean hasMore = (numResponses < tailMap.size());
    return new BatchedListEntries<CompressionZone>(zones, hasMore);
  }

  /**
   * Resolves the path to inode id, then check if it's the same as the inode id
   * passed in. This is necessary to filter out zones in snapshots.
   * @param zoneId
   * @param zonePath
   * @return true if path resolve to the id, false if not.
   * @throws AccessControlException
   * @throws ParentNotDirectoryException
   * @throws UnresolvedLinkException
   */
  private boolean pathResolvesToId(final long zoneId, final String zonePath)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    assert dir.hasReadLock();
    INode inode = dir.getInode(zoneId);
    if (inode == null) {
      return false;
    }
    INode lastINode = null;
    if (INode.isValidAbsolutePath(zonePath)) {
      INodesInPath iip = dir.getINodesInPath(zonePath, DirOp.READ_LINK);
      lastINode = iip.getLastINode();
    }
    if (lastINode == null || lastINode.getId() != zoneId) {
      return false;
    }
    return true;
  }

  /**
   * Return whether an INode is an compression zone root.
   * @param inode
   * @param name
   * @return true when INode is an compression zone root else false
   * @throws FileNotFoundException
   */
  boolean isCompressionZoneRoot(final INode inode, final String name)
      throws FileNotFoundException {
    assert dir.hasReadLock();
    if (inode == null) {
      throw new FileNotFoundException("INode does not exist for " + name);
    }
    if (!inode.isDirectory()) {
      return false;
    }
    if (!hasCreatedCompressionZone()
        || !compressionZones.containsKey(inode.getId())) {
      return false;
    }
    return true;
  }

  /**
   * Return whether an INode is an compression zone root.
   *
   * @param inode the zone inode
   * @param name
   * @throws IOException if the inode is not a directory,
   *                     or is a directory but not the root of an CZ.
   */
  void checkCompressionZoneRoot(final INode inode, final String name)
      throws IOException {
    if (!isCompressionZoneRoot(inode, name)) {
      throw new IOException("Path " + name + " is not the root of an"
          + " compression zone.");
    }
  }

  /**
   * @return number of compression zones.
   */
  public int getNumCompressionZones() {
    return hasCreatedCompressionZone() ?
        compressionZones.size() : 0;
  }

  /**
   * @return Whether there has been any attempt to create an compression zone in
   * the cluster at all. If not, it is safe to quickly return null when
   * checking the compression information of any file or directory in the
   * cluster.
   */
  public boolean hasCreatedCompressionZone() {
    return compressionZones != null;
  }

}