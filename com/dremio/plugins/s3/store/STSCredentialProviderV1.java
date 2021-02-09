package com.dremio.plugins.s3.store;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.Builder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;

public class STSCredentialProviderV1 implements AWSCredentialsProvider, Closeable {
   private final STSAssumeRoleSessionCredentialsProvider stsAssumeRoleSessionCredentialsProvider;

   public STSCredentialProviderV1(URI uri, Configuration conf) throws IOException {
      AWSCredentialsProvider awsCredentialsProvider = null;
      if ("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".equals(conf.get("fs.s3a.assumed.role.credentials.provider"))) {
         awsCredentialsProvider = new SimpleAWSCredentialsProvider(uri, conf);
      } else if ("com.amazonaws.auth.InstanceProfileCredentialsProvider".equals(conf.get("fs.s3a.assumed.role.credentials.provider"))) {
         awsCredentialsProvider = InstanceProfileCredentialsProvider.getInstance();
      }

      String region = S3FileSystem.getAWSRegionFromConfigurationOrDefault(conf).toString();
      AWSSecurityTokenServiceClientBuilder builder = (AWSSecurityTokenServiceClientBuilder)((AWSSecurityTokenServiceClientBuilder)((AWSSecurityTokenServiceClientBuilder)AWSSecurityTokenServiceClientBuilder.standard().withCredentials((AWSCredentialsProvider)awsCredentialsProvider)).withClientConfiguration(S3AUtils.createAwsConf(conf, ""))).withRegion(region);
      S3FileSystem.getStsEndpoint(conf).ifPresent((e) -> {
         builder.withEndpointConfiguration(new EndpointConfiguration(e, region));
      });
      this.stsAssumeRoleSessionCredentialsProvider = (new Builder(conf.get("fs.s3a.assumed.role.arn"), UUID.randomUUID().toString())).withStsClient((AWSSecurityTokenService)builder.build()).build();
   }

   public AWSCredentials getCredentials() {
      return this.stsAssumeRoleSessionCredentialsProvider.getCredentials();
   }

   public void refresh() {
      this.stsAssumeRoleSessionCredentialsProvider.refresh();
   }

   public void close() throws IOException {
      this.stsAssumeRoleSessionCredentialsProvider.close();
   }
}
