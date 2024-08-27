package org.apache.hadoop.compress;

import java.io.IOException;
import java.util.ArrayList;

public interface CompressIndexWriter {
  void writeIndex(ArrayList<Long> uncompressedIndexes, ArrayList<Long> compressedIndexes) throws IOException;
}
