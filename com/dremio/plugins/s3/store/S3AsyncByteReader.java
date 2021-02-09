package com.dremio.plugins.s3.store;

import com.dremio.io.ReusableAsyncByteReader;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class S3AsyncByteReader extends ReusableAsyncByteReader {
   private static final Logger logger = LoggerFactory.getLogger(S3AsyncByteReader.class);
   private static final AtomicInteger numOutstandingReads = new AtomicInteger(0);
   private final S3AsyncClient client;
   private final String bucket;
   private final String path;

   public S3AsyncByteReader(S3AsyncClient client, String bucket, String path) {
      this.client = client;
      this.bucket = bucket;
      this.path = path;
   }

   public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
      if (len == 0) {
         throw new IllegalArgumentException("Empty reads not allowed.");
      } else {
         logger.debug("Starting read of {}.{} for range {}..{}", new Object[]{this.bucket, this.path, offset, offset + (long)len});
         Stopwatch w = Stopwatch.createStarted();
         numOutstandingReads.incrementAndGet();
         CompletableFuture<Void> future = this.client.getObject((GetObjectRequest)GetObjectRequest.builder().range(range(offset, (long)len)).bucket(this.bucket).key(this.path).build(), new S3AsyncByteReader.ByteRangeReader(dst, dstOffset));
         return future.whenComplete((a, b) -> {
            int numOutstanding = numOutstandingReads.decrementAndGet();
            if (b == null) {
               logger.debug("Finished read of {}.{} for range {}..{} in {}ms.", new Object[]{this.bucket, this.path, offset, offset + (long)len, w.elapsed(TimeUnit.MILLISECONDS)});
            } else {
               logger.warn("Async read of {}.{} for length {} failed in {}ms when there are {} outstanding reads. Error {}", new Object[]{this.bucket, this.path, len, w.elapsed(TimeUnit.MILLISECONDS), numOutstanding, b});
            }
         });
      }
   }

   protected static String range(long start, long len) {
      return String.format("bytes=%d-%d", start, start + len - 1L);
   }

   private static class ByteRangeReader implements AsyncResponseTransformer<GetObjectResponse, Void> {
      private int curOffset;
      private final ByteBuf dst;
      private final CompletableFuture<Void> future = new CompletableFuture();
      private GetObjectResponse response;

      public ByteRangeReader(ByteBuf dst, int dstOffset) {
         this.dst = dst;
         this.curOffset = dstOffset;
      }

      public CompletableFuture<Void> prepare() {
         return this.future;
      }

      public void onResponse(GetObjectResponse response) {
         this.response = response;
      }

      public void onStream(SdkPublisher<ByteBuffer> publisher) {
         publisher.subscribe(new Subscriber<ByteBuffer>() {
            public void onSubscribe(Subscription s) {
               s.request(Long.MAX_VALUE);
            }

            public void onNext(ByteBuffer b) {
               int len = b.remaining();
               ByteRangeReader.this.dst.setBytes(ByteRangeReader.this.curOffset, b);
               ByteRangeReader.this.curOffset = len;
            }

            public void onError(Throwable error) {
               ByteRangeReader.this.future.completeExceptionally(error);
            }

            public void onComplete() {
               ByteRangeReader.this.future.complete((Object)null);
            }
         });
      }

      public void exceptionOccurred(Throwable error) {
         this.future.completeExceptionally(error);
      }
   }
}
