package org.apache.hadoop.compress;

import java.io.IOException;
import java.util.Map;

public interface CompressionIndexListener {
    void addCompressionIndex(String src, Map<Long, Long> indexMap) throws IOException;
}
