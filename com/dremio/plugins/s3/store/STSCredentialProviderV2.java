package com.dremio.plugins.s3.store;

import com.dremio.aws.SharedInstanceProfileCredentialsProvider;
import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider.Builder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.SdkAutoCloseable;

public class STSCredentialProviderV2 implements AwsCredentialsProvider, SdkAutoCloseable {
   private final StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider;

   public STSCredentialProviderV2(Configuration conf) {
      AwsCredentialsProvider awsCredentialsProvider = null;
      if ("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".equals(conf.get("fs.s3a.assumed.role.credentials.provider"))) {
         awsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(conf.get("fs.s3a.access.key"), conf.get("fs.s3a.secret.key")));
      } else if ("com.amazonaws.auth.InstanceProfileCredentialsProvider".equals(conf.get("fs.s3a.assumed.role.credentials.provider"))) {
         awsCredentialsProvider = new SharedInstanceProfileCredentialsProvider();
      }

      StsClientBuilder builder = (StsClientBuilder)((StsClientBuilder)((StsClientBuilder)StsClient.builder().credentialsProvider((AwsCredentialsProvider)awsCredentialsProvider)).region(S3FileSystem.getAWSRegionFromConfigurationOrDefault(conf))).httpClientBuilder(initConnectionSettings(conf));
      S3FileSystem.getStsEndpoint(conf).ifPresent((e) -> {
         try {
            builder.endpointOverride(new URI(e));
         } catch (URISyntaxException var3) {
            throw UserException.sourceInBadState(var3).buildSilently();
         }
      });
      initUserAgent(builder, conf);
      AssumeRoleRequest assumeRoleRequest = (AssumeRoleRequest)AssumeRoleRequest.builder().roleArn(conf.get("fs.s3a.assumed.role.arn")).roleSessionName(UUID.randomUUID().toString()).build();
      this.stsAssumeRoleCredentialsProvider = (StsAssumeRoleCredentialsProvider)((Builder)StsAssumeRoleCredentialsProvider.builder().refreshRequest(assumeRoleRequest).stsClient((StsClient)builder.build())).build();
   }

   public AwsCredentials resolveCredentials() {
      return this.stsAssumeRoleCredentialsProvider.resolveCredentials();
   }

   public static software.amazon.awssdk.http.SdkHttpClient.Builder<?> initConnectionSettings(Configuration conf) {
      software.amazon.awssdk.http.apache.ApacheHttpClient.Builder httpBuilder = ApacheHttpClient.builder();
      httpBuilder.maxConnections(intOption(conf, "fs.s3a.connection.maximum", 15, 1));
      httpBuilder.connectionTimeout(Duration.ofSeconds((long)intOption(conf, "fs.s3a.connection.establish.timeout", 50000, 0)));
      httpBuilder.socketTimeout(Duration.ofSeconds((long)intOption(conf, "fs.s3a.connection.timeout", 200000, 0)));
      httpBuilder.proxyConfiguration(initProxySupport(conf));
      return httpBuilder;
   }

   public static ProxyConfiguration initProxySupport(Configuration conf) throws IllegalArgumentException {
      software.amazon.awssdk.http.apache.ProxyConfiguration.Builder builder = ProxyConfiguration.builder();
      String proxyHost = conf.getTrimmed("fs.s3a.proxy.host", "");
      int proxyPort = conf.getInt("fs.s3a.proxy.port", -1);
      if (!proxyHost.isEmpty()) {
         if (proxyPort < 0) {
            if (conf.getBoolean("fs.s3a.connection.ssl.enabled", true)) {
               proxyPort = 443;
            } else {
               proxyPort = 80;
            }
         }

         builder.endpoint(URI.create(proxyHost + ":" + proxyPort));

         try {
            String proxyUsername = lookupPassword(conf, "fs.s3a.proxy.username");
            String proxyPassword = lookupPassword(conf, "fs.s3a.proxy.password");
            if (proxyUsername == null != (proxyPassword == null)) {
               throw new IllegalArgumentException(String.format("Proxy error: %s or %s set without the other.", "fs.s3a.proxy.username", "fs.s3a.proxy.password"));
            }

            builder.username(proxyUsername);
            builder.password(proxyPassword);
            builder.ntlmDomain(conf.getTrimmed("fs.s3a.proxy.domain"));
            builder.ntlmWorkstation(conf.getTrimmed("fs.s3a.proxy.workstation"));
         } catch (IOException var6) {
            throw UserException.sourceInBadState(var6).buildSilently();
         }
      } else if (proxyPort >= 0) {
         throw new IllegalArgumentException(String.format("Proxy error: %s set without %s", "fs.s3a.proxy.host", "fs.s3a.proxy.port"));
      }

      return (ProxyConfiguration)builder.build();
   }

   private static void initUserAgent(StsClientBuilder builder, Configuration conf) {
      String userAgent = "Hadoop " + VersionInfo.getVersion();
      String userAgentPrefix = conf.getTrimmed("fs.s3a.user.agent.prefix", "");
      if (!userAgentPrefix.isEmpty()) {
         userAgent = userAgentPrefix + ", " + userAgent;
      }

      builder.overrideConfiguration((ClientOverrideConfiguration)ClientOverrideConfiguration.builder().putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent).build());
   }

   static int intOption(Configuration conf, String key, int defVal, int min) {
      int v = conf.getInt(key, defVal);
      Preconditions.checkArgument(v >= min, "Value of %s: %s is below the minimum value %s", key, v, min);
      return v;
   }

   static String lookupPassword(Configuration conf, String key) throws IOException {
      try {
         char[] pass = conf.getPassword(key);
         return pass != null ? (new String(pass)).trim() : null;
      } catch (IOException var3) {
         throw new IOException("Cannot find password option " + key, var3);
      }
   }

   public void close() {
      this.stsAssumeRoleCredentialsProvider.close();
   }
}
