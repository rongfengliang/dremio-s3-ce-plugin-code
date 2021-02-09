package com.dremio.plugins.s3.store;

import com.amazonaws.SdkBaseException;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.io.ReusableAsyncByteReader;
import com.dremio.plugins.util.CloseableRef;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder;

class S3AsyncByteReaderUsingSyncClient extends ReusableAsyncByteReader {
   private static final Logger logger = LoggerFactory.getLogger(S3AsyncByteReaderUsingSyncClient.class);
   private static final ExecutorService threadPool = Executors.newCachedThreadPool(new NamedThreadFactory("s3-read-"));
   private final CloseableRef<S3Client> s3Ref;
   private final S3Client s3;
   private final String bucket;
   private final String path;
   private final Instant instant;
   private final String threadName;
   private final boolean requesterPays;

   S3AsyncByteReaderUsingSyncClient(CloseableRef<S3Client> s3Ref, String bucket, String path, String version, boolean requesterPays) {
      this.s3Ref = s3Ref;
      this.s3 = (S3Client)s3Ref.acquireRef();
      this.bucket = bucket;
      this.path = path;
      this.instant = Instant.ofEpochMilli(Long.parseLong(version));
      this.threadName = Thread.currentThread().getName();
      this.requesterPays = requesterPays;
   }

   public CompletableFuture<Void> readFully(long offset, ByteBuf dstBuf, int dstOffset, int len) {
      S3AsyncByteReaderUsingSyncClient.S3SyncReadObject readRequest = new S3AsyncByteReaderUsingSyncClient.S3SyncReadObject(offset, len, dstBuf, dstOffset);
      logger.debug("[{}] Submitted request to queue for bucket {}, path {} for {}", new Object[]{this.threadName, this.bucket, this.path, S3AsyncByteReader.range(offset, (long)len)});
      return CompletableFuture.runAsync(readRequest, threadPool);
   }

   public void onClose() throws Exception {
      this.s3Ref.close();
   }

   class S3SyncReadObject implements Runnable {
      private final ByteBuf byteBuf;
      private final int dstOffset;
      private final long offset;
      private final int len;
      private final S3AsyncByteReaderUsingSyncClient.RetryableInvoker invoker;

      S3SyncReadObject(long offset, int len, ByteBuf byteBuf, int dstOffset) {
         this.offset = offset;
         this.len = len;
         this.byteBuf = byteBuf;
         this.dstOffset = dstOffset;
         this.invoker = new S3AsyncByteReaderUsingSyncClient.RetryableInvoker(1);
      }

      public void run() {
         Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(S3FileSystem.class);
         Throwable var2 = null;

         try {
            Builder requestBuilder = GetObjectRequest.builder().bucket(S3AsyncByteReaderUsingSyncClient.this.bucket).key(S3AsyncByteReaderUsingSyncClient.this.path).range(S3AsyncByteReader.range(this.offset, (long)this.len)).ifUnmodifiedSince(S3AsyncByteReaderUsingSyncClient.this.instant);
            if (S3AsyncByteReaderUsingSyncClient.this.requesterPays) {
               requestBuilder.requestPayer("requester");
            }

            GetObjectRequest request = (GetObjectRequest)requestBuilder.build();
            Stopwatch watch = Stopwatch.createStarted();

            try {
               ResponseBytes<GetObjectResponse> responseBytes = (ResponseBytes)this.invoker.invoke(() -> {
                  return S3AsyncByteReaderUsingSyncClient.this.s3.getObjectAsBytes(request);
               });
               this.byteBuf.setBytes(this.dstOffset, responseBytes.asInputStream(), this.len);
               S3AsyncByteReaderUsingSyncClient.logger.debug("[{}] Completed request for bucket {}, path {} for {}, took {} ms", new Object[]{S3AsyncByteReaderUsingSyncClient.this.threadName, S3AsyncByteReaderUsingSyncClient.this.bucket, S3AsyncByteReaderUsingSyncClient.this.path, request.range(), watch.elapsed(TimeUnit.MILLISECONDS)});
            } catch (S3Exception var16) {
               switch(var16.statusCode()) {
               case 403:
                  S3AsyncByteReaderUsingSyncClient.logger.info("[{}] Request for bucket {}, path {} failed as access was denied, took {} ms", new Object[]{S3AsyncByteReaderUsingSyncClient.this.threadName, S3AsyncByteReaderUsingSyncClient.this.bucket, S3AsyncByteReaderUsingSyncClient.this.path, watch.elapsed(TimeUnit.MILLISECONDS)});
                  throw UserException.permissionError(var16).message("Access was denied by S3", new Object[0]).build(S3AsyncByteReaderUsingSyncClient.logger);
               case 412:
                  S3AsyncByteReaderUsingSyncClient.logger.info("[{}] Request for bucket {}, path {} failed as requested version of file not present, took {} ms", new Object[]{S3AsyncByteReaderUsingSyncClient.this.threadName, S3AsyncByteReaderUsingSyncClient.this.bucket, S3AsyncByteReaderUsingSyncClient.this.path, watch.elapsed(TimeUnit.MILLISECONDS)});
                  throw new CompletionException(new FileNotFoundException("Version of file changed " + S3AsyncByteReaderUsingSyncClient.this.path));
               default:
                  S3AsyncByteReaderUsingSyncClient.logger.error("[{}] Request for bucket {}, path {} failed with code {}. Failing read, took {} ms", new Object[]{S3AsyncByteReaderUsingSyncClient.this.threadName, S3AsyncByteReaderUsingSyncClient.this.bucket, S3AsyncByteReaderUsingSyncClient.this.path, var16.statusCode(), watch.elapsed(TimeUnit.MILLISECONDS)});
                  throw new CompletionException(var16);
               }
            } catch (Exception var17) {
               S3AsyncByteReaderUsingSyncClient.logger.error("[{}] Failed request for bucket {}, path {} for {}, took {} ms", new Object[]{S3AsyncByteReaderUsingSyncClient.this.threadName, S3AsyncByteReaderUsingSyncClient.this.bucket, S3AsyncByteReaderUsingSyncClient.this.path, request.range(), watch.elapsed(TimeUnit.MILLISECONDS), var17});
               throw new CompletionException(var17);
            }
         } catch (Throwable var18) {
            var2 = var18;
            throw var18;
         } finally {
            if (swapper != null) {
               if (var2 != null) {
                  try {
                     swapper.close();
                  } catch (Throwable var15) {
                     var2.addSuppressed(var15);
                  }
               } else {
                  swapper.close();
               }
            }

         }

      }
   }

   static class RetryableInvoker {
      private final int maxRetries;

      RetryableInvoker(int maxRetries) {
         this.maxRetries = maxRetries;
      }

      <T> T invoke(Callable<T> operation) throws Exception {
         int retryCount = 0;

         while(true) {
            try {
               return operation.call();
            } catch (IOException | RetryableException | SdkBaseException var4) {
               if (retryCount >= this.maxRetries) {
                  throw var4;
               }

               S3AsyncByteReaderUsingSyncClient.logger.warn("Retrying S3Async operation, exception was: {}", var4.getLocalizedMessage());
               ++retryCount;
            }
         }
      }
   }
}
