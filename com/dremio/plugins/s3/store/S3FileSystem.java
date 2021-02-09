package com.dremio.plugins.s3.store;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Retryer;
import com.dremio.common.util.Retryer.Builder;
import com.dremio.common.util.Retryer.OperationFailedAfterRetriesException;
import com.dremio.common.util.Retryer.WaitStrategy;
import com.dremio.exec.hadoop.MayProvideAsyncStream;
import com.dremio.exec.store.dfs.DremioFileSystemCache;
import com.dremio.exec.store.dfs.FileSystemConf.CloudFileSystemScheme;
import com.dremio.io.AsyncByteReader;
import com.dremio.plugins.util.CloseableRef;
import com.dremio.plugins.util.CloseableResource;
import com.dremio.plugins.util.ContainerAccessDeniedException;
import com.dremio.plugins.util.ContainerFileSystem;
import com.dremio.plugins.util.ContainerNotFoundException;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;

public class S3FileSystem extends ContainerFileSystem implements MayProvideAsyncStream {
   public static final String S3_PERMISSION_ERROR_MSG = "Access was denied by S3";
   static final String COMPATIBILITY_MODE = "dremio.s3.compat";
   static final String REGION_OVERRIDE = "dremio.s3.region";
   private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
   private static final String S3_URI_SCHEMA = "s3a://";
   private static final URI S3_URI = URI.create("s3a://aws");
   private static final String S3_ENDPOINT_END = ".amazonaws.com";
   private static final String S3_CN_ENDPOINT_END = ".amazonaws.com.cn";
   private final Retryer retryer;
   private final LoadingCache<String, CloseableRef<S3Client>> syncClientCache;
   private final LoadingCache<S3FileSystem.S3ClientKey, CloseableResource<AmazonS3>> clientCache;
   private S3FileSystem.S3ClientKey clientKey;
   private final DremioFileSystemCache fsCache;
   private boolean useWhitelistedBuckets;
   private static final Predicate<ContainerFileSystem.CorrectableFileStatus> ELIMINATE_PARENT_DIRECTORY = (input) -> {
      FileStatus status = input.getStatus();
      if (!status.isDirectory()) {
         return true;
      } else {
         return !Path.getPathWithoutSchemeAndAuthority(input.getPathWithoutContainerName()).equals(Path.getPathWithoutSchemeAndAuthority(status.getPath()));
      }
   };

   public S3FileSystem() {
      super(CloudFileSystemScheme.S3_FILE_SYSTEM_SCHEME.getScheme(), "bucket", ELIMINATE_PARENT_DIRECTORY);
      this.retryer = (new Builder()).retryIfExceptionOfType(SdkClientException.class).retryIfExceptionOfType(software.amazon.awssdk.core.exception.SdkClientException.class).setWaitStrategy(WaitStrategy.EXPONENTIAL, 250, 2500).setMaxRetries(10).build();
      this.syncClientCache = CacheBuilder.newBuilder().expireAfterAccess(1L, TimeUnit.HOURS).removalListener((notification) -> {
         AutoCloseables.close(RuntimeException.class, new AutoCloseable[]{(AutoCloseable)notification.getValue()});
      }).build(new CacheLoader<String, CloseableRef<S3Client>>() {
         public CloseableRef<S3Client> load(String bucket) {
            S3Client syncClient = (S3Client)((S3ClientBuilder)S3FileSystem.this.configClientBuilder(S3Client.builder(), bucket)).build();
            return new CloseableRef(syncClient);
         }
      });
      this.clientCache = CacheBuilder.newBuilder().expireAfterAccess(1L, TimeUnit.HOURS).maximumSize(20L).removalListener((notification) -> {
         AutoCloseables.close(RuntimeException.class, new AutoCloseable[]{(AutoCloseable)notification.getValue()});
      }).build(new CacheLoader<S3FileSystem.S3ClientKey, CloseableResource<AmazonS3>>() {
         public CloseableResource<AmazonS3> load(S3FileSystem.S3ClientKey clientKey) throws Exception {
            S3FileSystem.logger.debug("Opening S3 client connection for {}", clientKey);
            DefaultS3ClientFactory clientFactory = new DefaultS3ClientFactory();
            clientFactory.setConf(clientKey.s3Config);
            AWSCredentialProviderList credentialsProvider = S3AUtils.createAWSCredentialProviderSet(S3FileSystem.S3_URI, clientKey.s3Config);
            AmazonS3 s3Client = clientFactory.createS3Client(S3FileSystem.S3_URI, "", credentialsProvider);
            AutoCloseable closeableCredProvider = credentialsProvider instanceof AutoCloseable ? credentialsProvider : () -> {
            };
            Consumer<AmazonS3> closeFunc = (s3) -> {
               AutoCloseables.close(RuntimeException.class, new AutoCloseable[]{() -> {
                  s3.shutdown();
               }, closeableCredProvider});
            };
            CloseableResource<AmazonS3> closeableS3 = new CloseableResource(s3Client, closeFunc);
            return closeableS3;
         }
      });
      this.fsCache = new DremioFileSystemCache();
   }

   protected void setup(Configuration conf) throws IOException {
      this.clientKey = S3FileSystem.S3ClientKey.create(conf);
      this.useWhitelistedBuckets = !conf.get("dremio.s3.whitelisted.buckets", "").isEmpty();
      if (!"org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".equals(conf.get("fs.s3a.aws.credentials.provider")) && !conf.getBoolean("dremio.s3.compat", false)) {
         this.verifyCredentials(conf);
      }

   }

   protected void verifyCredentials(Configuration conf) throws RuntimeException {
      AwsCredentialsProvider awsCredentialsProvider = this.getAsync2Provider(conf);
      StsClientBuilder stsClientBuilder = (StsClientBuilder)((StsClientBuilder)StsClient.builder().credentialsProvider(awsCredentialsProvider)).region(getAWSRegionFromConfigurationOrDefault(conf));

      try {
         StsClient stsClient = (StsClient)stsClientBuilder.build();
         Throwable var5 = null;

         try {
            this.retryer.call(() -> {
               GetCallerIdentityRequest request = (GetCallerIdentityRequest)GetCallerIdentityRequest.builder().build();
               stsClient.getCallerIdentity(request);
               return true;
            });
         } catch (Throwable var11) {
            var5 = var11;
            throw var11;
         } finally {
            if (stsClient != null) {
               $closeResource(var5, stsClient);
            }

         }

      } catch (OperationFailedAfterRetriesException var13) {
         throw new RuntimeException("Credential Verification failed.", var13);
      }
   }

   public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
      return super.listFiles(f, recursive);
   }

   protected Stream<ContainerFileSystem.ContainerCreator> getContainerCreators() throws IOException {
      try {
         CloseableResource<AmazonS3> s3Ref = this.getS3V1Client();
         Throwable var2 = null;

         Stream var5;
         try {
            s3Ref.incrementRef();
            AmazonS3 s3 = (AmazonS3)s3Ref.getResource();
            Stream<String> buckets = this.getBucketNamesFromConfigurationProperty("dremio.s3.external.buckets");
            if (!"org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".equals(this.getConf().get("fs.s3a.aws.credentials.provider"))) {
               if (!this.useWhitelistedBuckets) {
                  buckets = Stream.concat(buckets, s3.listBuckets().stream().map(Bucket::getName));
               } else {
                  buckets = Stream.concat(buckets, this.getBucketNamesFromConfigurationProperty("dremio.s3.whitelisted.buckets"));
               }
            }

            var5 = buckets.distinct().map((input) -> {
               return new S3FileSystem.BucketCreator(this.getConf(), input);
            });
         } catch (Throwable var10) {
            var2 = var10;
            throw var10;
         } finally {
            if (s3Ref != null) {
               $closeResource(var2, s3Ref);
            }

         }

         return var5;
      } catch (Exception var12) {
         logger.error("Error while listing S3 buckets", var12);
         throw new IOException(var12);
      }
   }

   private Stream<String> getBucketNamesFromConfigurationProperty(String bucketConfigurationProperty) {
      String bucketList = this.getConf().get(bucketConfigurationProperty, "");
      return Arrays.stream(bucketList.split(",")).map(String::trim).filter((input) -> {
         return !Strings.isNullOrEmpty(input);
      });
   }

   protected ContainerFileSystem.ContainerHolder getUnknownContainer(String containerName) throws IOException {
      boolean containerFound = false;

      try {
         CloseableResource<AmazonS3> s3Ref = this.getS3V1Client();
         Throwable var4 = null;

         try {
            s3Ref.incrementRef();
            AmazonS3 s3 = (AmazonS3)s3Ref.getResource();
            if (s3.doesBucketExistV2(containerName)) {
               ListObjectsV2Request req = (new ListObjectsV2Request()).withMaxKeys(1).withRequesterPays(this.isRequesterPays()).withEncodingType("url").withBucketName(containerName);
               containerFound = s3.listObjectsV2(req).getBucketName().equals(containerName);
            }
         } catch (Throwable var12) {
            var4 = var12;
            throw var12;
         } finally {
            if (s3Ref != null) {
               $closeResource(var4, s3Ref);
            }

         }
      } catch (AmazonS3Exception var14) {
         if (var14.getMessage().contains("Access Denied")) {
            throw new ContainerAccessDeniedException("aws-bucket", containerName, var14);
         }

         throw new ContainerNotFoundException("Error while looking up bucket " + containerName, var14);
      } catch (Exception var15) {
         logger.error("Error while looking up bucket " + containerName, var15);
      }

      logger.debug("Unknown container '{}' found ? {}", containerName, containerFound);
      if (!containerFound) {
         throw new ContainerNotFoundException("Bucket " + containerName + " not found");
      } else {
         return (new S3FileSystem.BucketCreator(this.getConf(), containerName)).toContainerHolder();
      }
   }

   private Region getAWSBucketRegion(String bucketName) throws SdkClientException {
      try {
         CloseableResource<AmazonS3> s3Ref = this.getS3V1Client();
         Throwable var3 = null;

         Region var6;
         try {
            s3Ref.incrementRef();
            AmazonS3 s3 = (AmazonS3)s3Ref.getResource();
            String awsRegionName = com.amazonaws.services.s3.model.Region.fromValue(s3.getBucketLocation(bucketName)).toAWSRegion().getName();
            var6 = Region.of(awsRegionName);
         } catch (Throwable var12) {
            var3 = var12;
            throw var12;
         } finally {
            if (s3Ref != null) {
               $closeResource(var3, s3Ref);
            }

         }

         return var6;
      } catch (SdkClientException var14) {
         throw var14;
      } catch (Exception var15) {
         logger.error("Error while fetching bucket region", var15);
         throw new RuntimeException(var15);
      }
   }

   public boolean supportsAsync() {
      return true;
   }

   public AsyncByteReader getAsyncByteReader(Path path, String version) throws IOException {
      String bucket = ContainerFileSystem.getContainerName(path);
      String pathStr = ContainerFileSystem.pathWithoutContainer(path).toString();
      pathStr = pathStr.startsWith("/") ? pathStr.substring(1) : pathStr;
      return new S3AsyncByteReaderUsingSyncClient(this.getSyncClient(bucket), bucket, pathStr, version, this.isRequesterPays());
   }

   private CloseableRef<S3Client> getSyncClient(String bucket) throws IOException {
      try {
         return (CloseableRef)this.syncClientCache.get(bucket);
      } catch (SdkClientException | ExecutionException var5) {
         Throwable cause = var5.getCause();
         Object toChain;
         if (cause == null) {
            toChain = var5;
         } else {
            Throwables.throwIfInstanceOf(cause, UserException.class);
            Throwables.throwIfInstanceOf(cause, IOException.class);
            toChain = cause;
         }

         throw new IOException(String.format("Unable to create a sync S3 client for bucket %s", bucket), (Throwable)toChain);
      }
   }

   public void close() throws IOException {
      AutoCloseables.close(IOException.class, new AutoCloseable[]{() -> {
         super.close();
      }, () -> {
         this.fsCache.closeAll(true);
      }, () -> {
         invalidateCache(this.syncClientCache);
      }, () -> {
         invalidateCache(this.clientCache);
      }});
   }

   private static final void invalidateCache(Cache<?, ?> cache) {
      cache.invalidateAll();
      cache.cleanUp();
   }

   @VisibleForTesting
   protected AwsCredentialsProvider getAsync2Provider(Configuration config) {
      String var2 = config.get("fs.s3a.aws.credentials.provider");
      byte var3 = -1;
      switch(var2.hashCode()) {
      case -132569187:
         if (var2.equals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")) {
            var3 = 0;
         }
         break;
      case 267439426:
         if (var2.equals("org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")) {
            var3 = 2;
         }
         break;
      case 609424973:
         if (var2.equals("com.dremio.plugins.s3.store.STSCredentialProviderV1")) {
            var3 = 3;
         }
         break;
      case 618402205:
         if (var2.equals("com.amazonaws.auth.InstanceProfileCredentialsProvider")) {
            var3 = 1;
         }
      }

      switch(var3) {
      case 0:
         return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.get("fs.s3a.access.key"), config.get("fs.s3a.secret.key")));
      case 1:
         return new SharedInstanceProfileCredentialsProvider();
      case 2:
         return AnonymousCredentialsProvider.create();
      case 3:
         return new STSCredentialProviderV2(config);
      default:
         throw new IllegalStateException(config.get("fs.s3a.aws.credentials.provider"));
      }
   }

   private <T extends AwsClientBuilder<?, ?>> T configClientBuilder(T builder, String bucket) {
      Configuration conf = this.getConf();
      builder.credentialsProvider(this.getAsync2Provider(conf));
      Optional<String> endpoint = getEndpoint(conf);
      endpoint.ifPresent((e) -> {
         try {
            builder.endpointOverride(new URI(e));
         } catch (URISyntaxException var3) {
            throw UserException.sourceInBadState(var3).build(logger);
         }
      });
      if (!this.isCompatMode()) {
         builder.region(this.getAWSBucketRegion(bucket));
      } else {
         builder.region(getAWSRegionFromConfigurationOrDefault(conf));
      }

      return builder;
   }

   static Region getAWSRegionFromConfigurationOrDefault(Configuration conf) {
      String regionOverride = conf.getTrimmed("dremio.s3.region");
      return !Strings.isNullOrEmpty(regionOverride) ? Region.of(regionOverride) : getAwsRegionFromEndpoint(conf.get("fs.s3a.endpoint"));
   }

   static Region getAwsRegionFromEndpoint(String endpoint) {
      return (Region)Optional.ofNullable(endpoint).map((e) -> {
         return e.toLowerCase(Locale.ROOT);
      }).filter((e) -> {
         return e.endsWith(".amazonaws.com") || e.endsWith(".amazonaws.com.cn");
      }).flatMap((e) -> {
         return Region.regions().stream().filter((region) -> {
            return e.contains(region.id());
         }).findFirst();
      }).orElse(Region.US_EAST_1);
   }

   static Optional<String> getEndpoint(Configuration conf) {
      return Optional.ofNullable(conf.getTrimmed("fs.s3a.endpoint")).map((s) -> {
         return getHttpScheme(conf) + s;
      });
   }

   static Optional<String> getStsEndpoint(Configuration conf) {
      return Optional.ofNullable(conf.getTrimmed("fs.s3a.assumed.role.sts.endpoint")).map((s) -> {
         return "https://" + s;
      });
   }

   private static String getHttpScheme(Configuration conf) {
      return conf.getBoolean("fs.s3a.connection.ssl.enabled", true) ? "https://" : "http://";
   }

   private boolean isCompatMode() {
      return this.getConf().getBoolean("dremio.s3.compat", false);
   }

   @VisibleForTesting
   protected boolean isRequesterPays() {
      return this.getConf().getBoolean("fs.s3a.requester-pays.enabled", false);
   }

   @VisibleForTesting
   protected CloseableResource<AmazonS3> getS3V1Client() throws Exception {
      return (CloseableResource)this.clientCache.get(this.clientKey);
   }

   // $FF: synthetic method
   private static void $closeResource(Throwable x0, AutoCloseable x1) {
      if (x0 != null) {
         try {
            x1.close();
         } catch (Throwable var3) {
            x0.addSuppressed(var3);
         }
      } else {
         x1.close();
      }

   }

   public static final class S3ClientKey {
      private static final List<String> UNIQUE_PROPS = ImmutableList.of("fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.connection.ssl.enabled", "fs.s3a.endpoint", "fs.s3a.aws.credentials.provider", "fs.s3a.connection.maximum", "fs.s3a.attempts.maximum", "fs.s3a.connection.establish.timeout", "fs.s3a.connection.timeout", "fs.s3a.socket.send.buffer", "fs.s3a.socket.recv.buffer", "fs.s3a.signing-algorithm", new String[]{"fs.s3a.user.agent.prefix", "fs.s3a.proxy.host", "fs.s3a.proxy.port", "fs.s3a.proxy.domain", "fs.s3a.proxy.username", "fs.s3a.proxy.password", "fs.s3a.proxy.workstation", "fs.s3a.path.style.access", "dremio.s3.compat", "dremio.s3.region", "fs.s3a.assumed.role.arn", "fs.s3a.assumed.role.credentials.provider", "fs.s3a.requester-pays.enabled"});
      private final Configuration s3Config;

      public static S3FileSystem.S3ClientKey create(Configuration fsConf) {
         return new S3FileSystem.S3ClientKey(fsConf);
      }

      private S3ClientKey(Configuration s3Config) {
         this.s3Config = s3Config;
      }

      public boolean equals(Object o) {
         if (this == o) {
            return true;
         } else if (o != null && this.getClass() == o.getClass()) {
            S3FileSystem.S3ClientKey that = (S3FileSystem.S3ClientKey)o;
            Iterator var3 = UNIQUE_PROPS.iterator();

            String prop;
            do {
               if (!var3.hasNext()) {
                  return true;
               }

               prop = (String)var3.next();
            } while(Objects.equals(this.s3Config.get(prop), that.s3Config.get(prop)));

            return false;
         } else {
            return false;
         }
      }

      public int hashCode() {
         int hash = 1;

         String value;
         for(Iterator var2 = UNIQUE_PROPS.iterator(); var2.hasNext(); hash = 31 * hash + (value != null ? value.hashCode() : 0)) {
            String prop = (String)var2.next();
            value = this.s3Config.get(prop);
         }

         return hash;
      }

      public String toString() {
         return "[ Access Key=" + this.s3Config.get("fs.s3a.access.key") + ", Secret Key =*****, isSecure=" + this.s3Config.get("fs.s3a.connection.ssl.enabled") + " ]";
      }
   }

   private class BucketCreator extends ContainerFileSystem.ContainerCreator {
      private final Configuration parentConf;
      private final String bucketName;

      BucketCreator(Configuration parentConf, String bucketName) {
         this.parentConf = parentConf;
         this.bucketName = bucketName;
      }

      protected String getName() {
         return this.bucketName;
      }

      protected ContainerFileSystem.ContainerHolder toContainerHolder() throws IOException {
         return new ContainerFileSystem.ContainerHolder(this.bucketName, new ContainerFileSystem.FileSystemSupplier() {
            public FileSystem create() throws IOException {
               Optional<String> endpoint = S3FileSystem.getEndpoint(S3FileSystem.this.getConf());
               String targetEndpoint;
               if (S3FileSystem.this.isCompatMode() && endpoint.isPresent()) {
                  targetEndpoint = (String)endpoint.get();
               } else {
                  try {
                     CloseableResource<AmazonS3> s3Ref = S3FileSystem.this.getS3V1Client();
                     Throwable var4 = null;

                     try {
                        s3Ref.incrementRef();
                        AmazonS3 s3 = (AmazonS3)s3Ref.getResource();
                        String bucketRegion = s3.getBucketLocation(BucketCreator.this.bucketName);
                        String fallbackEndpoint = (String)endpoint.orElseGet(() -> {
                           return String.format("%ss3.%s.amazonaws.com", S3FileSystem.getHttpScheme(S3FileSystem.this.getConf()), bucketRegion);
                        });
                        String regionEndpoint = fallbackEndpoint;

                        try {
                           com.amazonaws.services.s3.model.Region region = com.amazonaws.services.s3.model.Region.fromValue(bucketRegion);
                           com.amazonaws.regions.Region awsRegion = region.toAWSRegion();
                           if (awsRegion != null) {
                              regionEndpoint = awsRegion.getServiceEndpoint("s3");
                           }
                        } catch (IllegalArgumentException var21) {
                           regionEndpoint = fallbackEndpoint;
                           S3FileSystem.logger.warn("Unknown or unmapped region {} for bucket {}. Will use following endpoint: {}", new Object[]{bucketRegion, BucketCreator.this.bucketName, fallbackEndpoint});
                        }

                        if (regionEndpoint == null) {
                           S3FileSystem.logger.error("Could not get AWSRegion for bucket {}. Will use following fs.s3a.endpoint: {} ", BucketCreator.this.bucketName, fallbackEndpoint);
                        }

                        targetEndpoint = regionEndpoint != null ? regionEndpoint : fallbackEndpoint;
                     } catch (Throwable var22) {
                        var4 = var22;
                        throw var22;
                     } finally {
                        if (s3Ref != null) {
                           if (var4 != null) {
                              try {
                                 s3Ref.close();
                              } catch (Throwable var20) {
                                 var4.addSuppressed(var20);
                              }
                           } else {
                              s3Ref.close();
                           }
                        }

                     }
                  } catch (AmazonS3Exception var24) {
                     if (var24.getStatusCode() == 403) {
                        throw UserException.permissionError(var24).message("Access was denied by S3", new Object[0]).build(S3FileSystem.logger);
                     }

                     throw var24;
                  } catch (Exception var25) {
                     S3FileSystem.logger.error("Error while creating container holder", var25);
                     throw new RuntimeException(var25);
                  }
               }

               String location = "s3a://" + BucketCreator.this.bucketName + "/";
               Configuration bucketConf = new Configuration(BucketCreator.this.parentConf);
               bucketConf.set("fs.s3a.endpoint", targetEndpoint);
               return S3FileSystem.this.fsCache.get((new Path(location)).toUri(), bucketConf, S3FileSystem.S3ClientKey.UNIQUE_PROPS);
            }
         });
      }
   }
}
