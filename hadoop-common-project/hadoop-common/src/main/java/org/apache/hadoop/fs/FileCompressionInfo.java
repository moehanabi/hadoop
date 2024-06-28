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
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.Serializable;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

/**
 * FileCompressionInfo encapsulates all the compression-related information for
 * an encrypted file.
 */
@InterfaceAudience.Private
public class FileCompressionInfo implements Serializable {

  private static final long serialVersionUID = 164020024006114548L;

  private final String compressionCodec;

  /**
   * Create a FileCompressionInfo.
   *
   * @param codec CompressionCodec used to compress the file
   */
  public FileCompressionInfo(final String codec) {
    checkNotNull(codec);
    this.compressionCodec = codec;
  }

  /**
   * @return {@link CompressionCodec} used to compress
   * the file.
   */
  public String getCompressionCodec() {
    return compressionCodec;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{")
        .append("compressionCodec: " + compressionCodec)
        .append("}");
    return builder.toString();
  }

  /**
   * A frozen version of {@link #toString()} to be backward compatible.
   * When backward compatibility is not needed, use {@link #toString()}, which
   * provides more info and is supposed to evolve.
   * Don't change this method except for major revisions.
   *
   * NOTE:
   * Currently this method is used by CLI for backward compatibility.
   *
   * @return stable string.
   */
  public String toStringStable() {
    StringBuilder builder = new StringBuilder("{")
        .append("compressionCodec: " + compressionCodec)
        .append("}");
    return builder.toString();
  }
}
