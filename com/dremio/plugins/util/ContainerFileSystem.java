package com.dremio.plugins.util;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.util.RemoteIterators;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList.Builder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ListAccessor;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ContainerFileSystem extends FileSystem {
   private static final Logger logger = LoggerFactory.getLogger(ContainerFileSystem.class);
   private final String scheme;
   private final String containerName;
   private final Object refreshLock = new Object();
   private final Predicate<ContainerFileSystem.CorrectableFileStatus> listFileStatusPredicate;
   private volatile ImmutableMap<String, ContainerFileSystem.ContainerHolder> containerMap = ImmutableMap.of();
   private volatile List<ContainerFileSystem.ContainerFailure> failures = ImmutableList.of();

   protected ContainerFileSystem(String scheme, String containerName, Predicate<ContainerFileSystem.CorrectableFileStatus> listFileStatusPredicate) {
      this.scheme = scheme;
      this.containerName = containerName;
      this.listFileStatusPredicate = listFileStatusPredicate;
   }

   public void refreshFileSystems() throws IOException {
      synchronized(this.refreshLock) {
         Map<String, ContainerFileSystem.ContainerHolder> oldMap = new HashMap(this.containerMap);
         Builder<ContainerFileSystem.ContainerFailure> failures = ImmutableList.builder();
         com.google.common.collect.ImmutableMap.Builder<String, ContainerFileSystem.ContainerHolder> newMap = ImmutableMap.builder();
         this.getContainerCreators().forEach((creator) -> {
            ContainerFileSystem.ContainerHolder fs = (ContainerFileSystem.ContainerHolder)oldMap.remove(creator.getName());
            if (fs != null) {
               newMap.put(creator.getName(), fs);
            } else {
               try {
                  newMap.put(creator.getName(), creator.toContainerHolder());
               } catch (Exception var7) {
                  logger.warn("Failure while attempting to connect to {} named [{}].", new Object[]{this.containerName, creator.getName(), var7});
                  failures.add(new ContainerFileSystem.ContainerFailure(creator.getName(), var7));
               }

            }
         });
         this.containerMap = newMap.build();
         Iterator var5 = oldMap.values().iterator();

         while(var5.hasNext()) {
            ContainerFileSystem.ContainerHolder old = (ContainerFileSystem.ContainerHolder)var5.next();

            try {
               if (this.getUnknownContainer(old.getName()) == null) {
                  logger.debug("Closing filesystem for the container {}", old.getName());
                  old.close();
               }
            } catch (IOException var9) {
               logger.warn("Failure while closing {} named [{}].", new Object[]{this.containerName, old.getName(), var9});
            }
         }

         this.failures = failures.build();
      }
   }

   public List<ContainerFileSystem.ContainerFailure> getSubFailures() {
      return this.failures;
   }

   public final void initialize(URI name, Configuration conf) throws IOException {
      super.initialize(name, conf);
      this.setup(conf);
      this.setConf(conf);
      this.refreshFileSystems();
   }

   public boolean containerExists(String containerName) {
      return this.containerMap.containsKey(containerName);
   }

   protected abstract void setup(Configuration var1) throws IOException;

   protected abstract Stream<ContainerFileSystem.ContainerCreator> getContainerCreators() throws IOException;

   protected abstract ContainerFileSystem.ContainerHolder getUnknownContainer(String var1) throws IOException;

   private boolean isRoot(Path path) {
      List<String> pathComponents = Arrays.asList(Path.getPathWithoutSchemeAndAuthority(path).toString().split("/"));
      return pathComponents.size() == 0;
   }

   public static String getContainerName(Path path) {
      List<String> pathComponents = Arrays.asList(PathUtils.removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString()).split("/"));
      return (String)pathComponents.get(0);
   }

   public static Path pathWithoutContainer(Path path) {
      List<String> pathComponents = Arrays.asList(PathUtils.removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString()).split("/"));
      return new Path("/" + Joiner.on("/").join(pathComponents.subList(1, pathComponents.size())));
   }

   private ContainerFileSystem.ContainerHolder getFileSystemForPath(Path path) throws IOException {
      String name = getContainerName(path);
      ContainerFileSystem.ContainerHolder container = (ContainerFileSystem.ContainerHolder)this.containerMap.get(name);
      if (container == null) {
         try {
            synchronized(this.refreshLock) {
               container = (ContainerFileSystem.ContainerHolder)this.containerMap.get(name);
               if (container == null) {
                  container = this.getUnknownContainer(name);
                  com.google.common.collect.ImmutableMap.Builder<String, ContainerFileSystem.ContainerHolder> newMap = ImmutableMap.builder();
                  newMap.putAll(this.containerMap);
                  newMap.put(name, container);
                  this.containerMap = newMap.build();
               }
            }
         } catch (IOException var8) {
            throw var8;
         } catch (Exception var9) {
            throw new IOException(String.format("Unable to retrieve %s named %s.", this.containerName, name), var9);
         }
      }

      return container;
   }

   public URI getUri() {
      try {
         return new URI(this.scheme + ":///");
      } catch (URISyntaxException var2) {
         throw new RuntimeException(var2);
      }
   }

   public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return this.getFileSystemForPath(f).fs().open(pathWithoutContainer(f), bufferSize);
   }

   public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
      return this.getFileSystemForPath(f).fs().create(pathWithoutContainer(f), permission, overwrite, bufferSize, replication, blockSize, progress);
   }

   public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
      return this.getFileSystemForPath(f).fs().append(pathWithoutContainer(f), bufferSize, progress);
   }

   public boolean rename(Path src, Path dst) throws IOException {
      Preconditions.checkArgument(getContainerName(src).equals(getContainerName(dst)), String.format("Cannot rename files across %ss.", this.containerName));
      return this.getFileSystemForPath(src).fs().rename(pathWithoutContainer(src), pathWithoutContainer(dst));
   }

   public boolean delete(Path f, boolean recursive) throws IOException {
      return this.getFileSystemForPath(f).fs().delete(pathWithoutContainer(f), recursive);
   }

   public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
      return super.listLocatedStatus(f);
   }

   protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException {
      String container = getContainerName(f);
      PathFilter alteredFilter = (path) -> {
         return filter.accept(transform(path, container));
      };
      return RemoteIterators.transform(ListAccessor.listLocatedFileStatus(this.getFileSystemForPath(f).fs(), pathWithoutContainer(f), alteredFilter), (t) -> {
         return new LocatedFileStatus(transform((FileStatus)t, container), t.getBlockLocations());
      });
   }

   public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
      String container = getContainerName(f);
      return RemoteIterators.transform(this.getFileSystemForPath(f).fs().listFiles(pathWithoutContainer(f), recursive), (t) -> {
         return new LocatedFileStatus(transform((FileStatus)t, container), t.getBlockLocations());
      });
   }

   public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
      if (this.isRoot(f)) {
         return (FileStatus[])this.containerMap.keySet().stream().map((containerNamex) -> {
            return new FileStatus(0L, true, 0, 0L, 0L, new Path(containerNamex));
         }).toArray((x$0) -> {
            return new FileStatus[x$0];
         });
      } else {
         String containerName = getContainerName(f);
         Path pathWithoutContainerName = pathWithoutContainer(f);
         return (FileStatus[])Arrays.stream(this.getFileSystemForPath(f).fs().listStatus(pathWithoutContainerName)).map((status) -> {
            return new ContainerFileSystem.CorrectableFileStatus(status, pathWithoutContainerName);
         }).filter(this.listFileStatusPredicate).map((input) -> {
            return transform(input.getStatus(), containerName);
         }).toArray((x$0) -> {
            return new FileStatus[x$0];
         });
      }
   }

   @VisibleForTesting
   static Path transform(Path path, String containerName) {
      String relativePath = PathUtils.removeLeadingSlash(Path.getPathWithoutSchemeAndAuthority(path).toString());
      Path containerPath = new Path("/" + containerName);
      return Strings.isNullOrEmpty(relativePath) ? containerPath : new Path(containerPath, new Path((String)null, (String)null, relativePath));
   }

   private static FileStatus transform(FileStatus input, String containerName) {
      return new FileStatus(input.getLen(), input.isDirectory(), input.getReplication(), input.getBlockSize(), input.getModificationTime(), input.getAccessTime(), input.getPermission(), input.getOwner(), input.getGroup(), transform(input.getPath(), containerName));
   }

   public void setWorkingDirectory(Path newDir) {
      throw new UnsupportedOperationException();
   }

   public Path getWorkingDirectory() {
      return new Path("/");
   }

   public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return this.getFileSystemForPath(f).fs().mkdirs(pathWithoutContainer(f), permission);
   }

   public boolean exists(Path f) throws IOException {
      try {
         return super.exists(f);
      } catch (ContainerNotFoundException var3) {
         return false;
      }
   }

   public FileStatus getFileStatus(Path f) throws IOException {
      if (this.isRoot(f)) {
         return new FileStatus(0L, true, 0, 0L, 0L, f);
      } else {
         FileStatus fileStatus = this.getFileSystemForPath(f).fs().getFileStatus(pathWithoutContainer(f));
         return transform(fileStatus, getContainerName(f));
      }
   }

   public String getScheme() {
      return this.scheme;
   }

   protected static class CorrectableFileStatus {
      private final FileStatus status;
      private final Path pathWithoutContainerName;

      public CorrectableFileStatus(FileStatus status, Path pathWithoutContainerName) {
         this.status = status;
         this.pathWithoutContainerName = pathWithoutContainerName;
      }

      public FileStatus getStatus() {
         return this.status;
      }

      public Path getPathWithoutContainerName() {
         return this.pathWithoutContainerName;
      }
   }

   public abstract static class FileSystemSupplier implements AutoCloseable {
      private volatile FileSystem fs;

      public final FileSystem get() throws IOException {
         if (this.fs != null) {
            return this.fs;
         } else {
            synchronized(this) {
               if (this.fs != null) {
                  return this.fs;
               } else {
                  this.fs = this.create();
                  return this.fs;
               }
            }
         }
      }

      public abstract FileSystem create() throws IOException;

      public void close() throws IOException {
         if (this.fs != null) {
            this.fs.close();
         }

      }
   }

   protected static class ContainerHolder implements AutoCloseable {
      private final String name;
      private final ContainerFileSystem.FileSystemSupplier fs;

      public ContainerHolder(String name, ContainerFileSystem.FileSystemSupplier fs) {
         this.name = name;
         this.fs = fs;
      }

      public String getName() {
         return this.name;
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.name});
      }

      public FileSystem fs() throws IOException {
         return this.fs.get();
      }

      public boolean equals(Object obj) {
         if (this == obj) {
            return true;
         } else if (obj == null) {
            return false;
         } else if (this.getClass() != obj.getClass()) {
            return false;
         } else {
            ContainerFileSystem.ContainerHolder other = (ContainerFileSystem.ContainerHolder)obj;
            return Objects.equals(this.name, other.name);
         }
      }

      public void close() throws IOException {
         this.fs.close();
      }
   }

   public abstract static class ContainerCreator {
      protected abstract String getName();

      protected abstract ContainerFileSystem.ContainerHolder toContainerHolder() throws IOException;
   }

   public static class ContainerFailure {
      private final String name;
      private final Exception exception;

      public ContainerFailure(String name, Exception exception) {
         this.name = name;
         this.exception = exception;
      }

      public String getName() {
         return this.name;
      }

      public Exception getException() {
         return this.exception;
      }
   }
}
