package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;

public final class ListAccessor {
   private ListAccessor() {
   }

   public static RemoteIterator<LocatedFileStatus> listLocatedFileStatus(FileSystem fs, Path path, PathFilter filter) throws FileNotFoundException, IOException {
      return fs.listLocatedStatus(path, filter);
   }
}
