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
package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileCompressionInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.CompressionZone;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

/**
 * This class implements compress command-line operations.
 */
@InterfaceAudience.Private
public class CompressAdmin extends Configured implements Tool {

  public CompressAdmin() {
    this(null);
  }

  public CompressAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws IOException {
    if (args.length == 0) {
      AdminHelper.printUsage(false, "compress", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    final AdminHelper.Command command = AdminHelper.determineCommand(args[0],
        COMMANDS);
    if (command == null) {
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      AdminHelper.printUsage(false, "compress", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    final List<String> argsList = new LinkedList<String>();
    for (int j = 1; j < args.length; j++) {
      argsList.add(args[j]);
    }
    try {
      return command.run(getConf(), argsList);
    } catch (IllegalArgumentException e) {
      System.err.println(prettifyException(e));
      return -1;
    }
  }

  public static void main(String[] argsArray) throws Exception {
    final CompressAdmin compressAdmin = new CompressAdmin(new Configuration());
    int res = ToolRunner.run(compressAdmin, argsArray);
    System.exit(res);
  }

  /**
   * NN exceptions contain the stack trace as part of the exception message.
   * When it's a known error, pretty-print the error and squish the stack trace.
   */
  private static String prettifyException(Exception e) {
    return e.getClass().getSimpleName() + ": " +
      e.getLocalizedMessage().split("\n")[0];
  }

  private static class CreateZoneCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-createZone";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -codec <codecName> -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the compression zone to create. " +
          "It must be an empty directory. A trash directory is provisioned " +
          "under this path.");
      listing.addRow("<codec>", "Name of the key to use for the " +
          "compression zone.");
      return getShortUsage() + "\n" +
          "Create a new compression zone.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("You must specify a path with -path.");
        return 1;
      }

      final String codec =
          StringUtils.popOptionWithArgument("-codec", args);
      if (codec == null) {
        System.err.println("You must specify a key name with -codec.");
        return 1;
      }

      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }
      Path p = new Path(path);
      HdfsAdmin admin = new HdfsAdmin(p.toUri(), conf);
      try {
        admin.createCompressionZone(p, codec);
        System.out.println("Added compression zone " + path);
      } catch (IOException e) {
        System.err.println(prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static class ListZonesCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-listZones";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName()+ "]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() + "\n" +
        "List all compression zones. Requires superuser permissions.\n\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }

      HdfsAdmin admin = new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
      try {
        final TableListing listing = new TableListing.Builder()
          .addField("").addField("", true)
          .hideHeaders().build();
        final RemoteIterator<CompressionZone> it = admin.listCompressionZones();
        while (it.hasNext()) {
          CompressionZone cz = it.next();
          listing.addRow(cz.getPath(), cz.getCodec());
        }
        System.out.println(listing.toString());
      } catch (IOException e) {
        System.err.println(prettifyException(e));
        return 2;
      }

      return 0;
    }
  }

  private static class GetFileCompressionInfoCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-getFileCompressionInfo";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path to the file to show compression info.");
      return getShortUsage() + "\n" + "Get compression info of a file.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);

      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }
      Path p = new Path(path);
      final HdfsAdmin admin =
          new HdfsAdmin(p.toUri(), conf);
      try {
        final FileCompressionInfo fci =
            admin.getFileCompressionInfo(p);
        if (fci == null) {
          System.err.println("No FileCompressionInfo found for path " + path);
          return 2;
        }
        System.out.println(fci.toStringStable());
      } catch (IOException e) {
        System.err.println(prettifyException(e));
        return 3;
      }
      return 0;
    }
  }

  private static final AdminHelper.Command[] COMMANDS = {
      new CreateZoneCommand(),
      new ListZonesCommand(),
      new GetFileCompressionInfoCommand(),
  };
}
