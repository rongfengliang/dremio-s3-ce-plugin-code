package com.dremio.plugins.s3.store;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.plugins.util.ContainerFileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.AccessMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.inject.Provider;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3StoragePlugin extends FileSystemPlugin<S3PluginConfig> {
   private static final Logger logger = LoggerFactory.getLogger(S3StoragePlugin.class);
   public static final int DEFAULT_MAX_CONNECTIONS = 1000;
   public static final String EXTERNAL_BUCKETS = "dremio.s3.external.buckets";
   public static final String WHITELISTED_BUCKETS = "dremio.s3.whitelisted.buckets";
   public static final String ACCESS_KEY_PROVIDER = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
   public static final String EC2_METADATA_PROVIDER = "com.amazonaws.auth.InstanceProfileCredentialsProvider";
   public static final String NONE_PROVIDER = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
   public static final String ASSUME_ROLE_PROVIDER = "com.dremio.plugins.s3.store.STSCredentialProviderV1";

   public S3StoragePlugin(S3PluginConfig config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
      super(config, context, name, idProvider);
   }

   protected List<Property> getProperties() {
      S3PluginConfig config = (S3PluginConfig)this.getConfig();
      List<Property> finalProperties = new ArrayList();
      finalProperties.add(new Property("fs.defaultFS", "dremioS3:///"));
      finalProperties.add(new Property("fs.dremioS3.impl", S3FileSystem.class.getName()));
      finalProperties.add(new Property("fs.s3a.connection.maximum", String.valueOf(1000)));
      finalProperties.add(new Property("fs.s3a.fast.upload", "true"));
      finalProperties.add(new Property("fs.s3a.fast.upload.buffer", "disk"));
      finalProperties.add(new Property("fs.s3a.fast.upload.active.blocks", "4"));
      finalProperties.add(new Property("fs.s3a.threads.max", "24"));
      finalProperties.add(new Property("fs.s3a.multipart.size", "67108864"));
      finalProperties.add(new Property("fs.s3a.max.total.tasks", "30"));
      if (config.compatibilityMode) {
         finalProperties.add(new Property("dremio.s3.compat", "true"));
      }

      String mainAWSCredProvider;
      switch(config.credentialType) {
      case ACCESS_KEY:
         if (!"".equals(config.accessKey) && !"".equals(config.accessSecret)) {
            mainAWSCredProvider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
            finalProperties.add(new Property("fs.s3a.access.key", config.accessKey));
            finalProperties.add(new Property("fs.s3a.secret.key", config.accessSecret));
            break;
         }

         throw UserException.validationError().message("Failure creating S3 connection. You must provide AWS Access Key and AWS Access Secret.", new Object[0]).build(logger);
      case EC2_METADATA:
         mainAWSCredProvider = "com.amazonaws.auth.InstanceProfileCredentialsProvider";
         break;
      case NONE:
         mainAWSCredProvider = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
         break;
      default:
         throw new RuntimeException("Failure creating S3 connection. Invalid credentials type.");
      }

      if (!Strings.isNullOrEmpty(config.assumedRoleARN) && !"org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".equals(mainAWSCredProvider)) {
         finalProperties.add(new Property("fs.s3a.assumed.role.arn", config.assumedRoleARN));
         finalProperties.add(new Property("fs.s3a.assumed.role.credentials.provider", mainAWSCredProvider));
         mainAWSCredProvider = "com.dremio.plugins.s3.store.STSCredentialProviderV1";
      }

      finalProperties.add(new Property("fs.s3a.aws.credentials.provider", mainAWSCredProvider));
      List<Property> propertyList = super.getProperties();
      if (propertyList != null && !propertyList.isEmpty()) {
         finalProperties.addAll(propertyList);
      }

      finalProperties.add(new Property("fs.s3a.connection.ssl.enabled", String.valueOf(config.secure)));
      if (config.externalBucketList != null && !config.externalBucketList.isEmpty()) {
         finalProperties.add(new Property("dremio.s3.external.buckets", Joiner.on(",").join(config.externalBucketList)));
      } else if (config.credentialType == AWSAuthenticationType.NONE) {
         throw UserException.validationError().message("Failure creating S3 connection. You must provide one or more external buckets when you choose no authentication.", new Object[0]).build(logger);
      }

      if (config.whitelistedBuckets != null && !config.whitelistedBuckets.isEmpty()) {
         finalProperties.add(new Property("dremio.s3.whitelisted.buckets", Joiner.on(",").join(config.whitelistedBuckets)));
      }

      if (!Strings.isNullOrEmpty(config.kmsKeyARN)) {
         finalProperties.add(new Property("fs.s3a.server-side-encryption.key", config.kmsKeyARN));
         finalProperties.add(new Property("fs.s3a.server-side-encryption-algorithm", S3AEncryptionMethods.SSE_KMS.getMethod()));
         finalProperties.add(new Property("fs.s3a.connection.ssl.enabled", Boolean.TRUE.toString()));
      }

      finalProperties.add(new Property("fs.s3a.requester-pays.enabled", Boolean.toString(config.requesterPays)));
      finalProperties.add(new Property("fs.s3a.create.file-status-check", Boolean.toString(config.enableFileStatusCheck)));
      logger.debug("getProperties: Create file status check: {}", config.enableFileStatusCheck);
      return finalProperties;
   }

   public boolean supportsColocatedReads() {
      return false;
   }

   public SourceState getState() {
      try {
         this.ensureDefaultName();
         S3FileSystem fs = (S3FileSystem)this.getSystemUserFS().unwrap(S3FileSystem.class);
         this.getSystemUserFS().access(((S3PluginConfig)this.getConfig()).getPath(), ImmutableSet.of(AccessMode.READ));
         fs.refreshFileSystems();
         List<ContainerFileSystem.ContainerFailure> failures = fs.getSubFailures();
         if (failures.isEmpty()) {
            return SourceState.GOOD;
         } else {
            StringBuilder sb = new StringBuilder();
            Iterator var4 = failures.iterator();

            while(var4.hasNext()) {
               ContainerFileSystem.ContainerFailure f = (ContainerFileSystem.ContainerFailure)var4.next();
               sb.append(f.getName());
               sb.append(": ");
               sb.append(f.getException().getMessage());
               sb.append("\n");
            }

            return SourceState.warnState(this.fileConnectionErrorMessage(((S3PluginConfig)this.getConfig()).getPath()), new String[]{sb.toString()});
         }
      } catch (Exception var6) {
         return SourceState.badState(this.fileConnectionErrorMessage(((S3PluginConfig)this.getConfig()).getPath()), var6);
      }
   }

   private String fileConnectionErrorMessage(Path filePath) {
      return String.format("Could not connect to %s. Check your S3 data source settings and credentials.", filePath.toString().equals("/") ? "S3 source" : filePath.toString());
   }

   private void ensureDefaultName() throws IOException {
      String urlSafeName = URLEncoder.encode(this.getName(), "UTF-8");
      this.getFsConf().set("fs.defaultFS", "dremioS3://" + urlSafeName);
      FileSystem fs = this.createFS("$dremio$");
      ((org.apache.hadoop.fs.FileSystem)fs.unwrap(org.apache.hadoop.fs.FileSystem.class)).initialize(URI.create(this.getFsConf().get("fs.defaultFS")), this.getFsConf());
   }

   public CreateTableEntry createNewTable(SchemaConfig config, NamespaceKey key, IcebergTableProps icebergTableProps, WriterOptions writerOptions, Map<String, Object> storageOptions) {
      Preconditions.checkArgument(key.size() >= 2, "key must be at least two parts");
      List<String> resolvedPath = this.resolveTableNameToValidPath(key.getPathComponents());
      String containerName = (String)resolvedPath.get(0);
      if (resolvedPath.size() == 1) {
         throw UserException.validationError().message("Creating buckets is not supported", new Object[]{containerName}).build(logger);
      } else {
         CreateTableEntry entry = super.createNewTable(config, key, icebergTableProps, writerOptions, storageOptions);
         S3FileSystem fs = (S3FileSystem)this.getSystemUserFS().unwrap(S3FileSystem.class);
         if (!fs.containerExists(containerName)) {
            throw UserException.validationError().message("Cannot create the table because '%s' bucket does not exist", new Object[]{containerName}).build(logger);
         } else {
            return entry;
         }
      }
   }

   protected boolean isAsyncEnabledForQuery(OperatorContext context) {
      return context != null && context.getOptions().getOption(S3Options.ASYNC);
   }
}
