For the latest information about Hadoop, please visit our website at:

   http://hadoop.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/HADOOP/

This bransh add the support of transparent compression. Now it has impelement the following features:

- Compress / Write / Output
   - Stream compress incoming data streams
   - Selective compression, compress only the compressible parts (compression ratio limit can be set, 80% for default)
   - Append. Support different buffer size from previous compression
   - Truncate IS NOT SUPPORTED because it runs completely in server
- Decompress / Read / Input
   - Identify the appropriate buffer size
   - Get full original data of compressed stream, that is
   - Randomly read
   - Positioned read
   - Seek
   - Readfully
   - Copy. Will decompress and/or recompress if source or destination are compression zones
   - Rename/move. Won't decompress and/or recompress. Will keep original compression algorithm
- Manage
   - Create compression zones and set compression algorithm for compression zones (`hdfs compress -createZone -codec <codecName> -path <path>`)
   - List all compression zones (`hdfs compress -listZones`)
   - Set default compression block size (`io.compression.codec.buffersize`)
   - Set compression ratio limit (`io.compression.ratio`)
- Work well with transparent encryption (TDE)
   - Can compress and decompress well with transparent encryption
   - For a certain directory, create compression zone first and then create encryption zone, because encryption zone creating will make a directory `.Trash` which will make the directory not empty
- Work well with command line, Java API and Web UI
- Can get correct original size with `ls`, `du` or Web UI
