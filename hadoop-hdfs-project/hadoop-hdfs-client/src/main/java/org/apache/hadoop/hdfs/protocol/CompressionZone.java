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
package org.apache.hadoop.hdfs.protocol;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A simple class for representing an compression zone. Presently an compression
 * zone only has a path (the root of the compression zone), a key name, and a
 * unique id. The id is used to implement batched listing of compression zones.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CompressionZone {

  private final long id;
  private final String path;
  private final String compressionCodec;

  public CompressionZone(long id, String path, String codec) {
    this.id = id;
    this.path = path;
    this.compressionCodec = codec;
  }

  public long getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  public String getCodec() {
    return compressionCodec;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(13, 31)
        .append(id)
        .append(path)
        .append(compressionCodec).
      toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }

    CompressionZone rhs = (CompressionZone) obj;
    return new EqualsBuilder().
      append(id, rhs.id).
      append(path, rhs.path).
      append(compressionCodec, rhs.compressionCodec).
      isEquals();
  }

  @Override
  public String toString() {
    return "CompressionZone [id=" + id +
        ", path=" + path +
        ", compressionCodec=" + compressionCodec + "]";
  }
}
