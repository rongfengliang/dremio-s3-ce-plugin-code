package com.dremio.plugins.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseableResource<T> implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(CloseableResource.class);
   private T resource;
   private Consumer<T> closerFunc;
   private AtomicInteger refCnt = new AtomicInteger(1);

   public CloseableResource(T resource, Consumer<T> closerFunc) {
      this.resource = resource;
      this.closerFunc = closerFunc;
   }

   public void incrementRef() {
      this.refCnt.incrementAndGet();
      if (logger.isDebugEnabled()) {
         logger.debug("Class {} acquired the ref for {}:{}, Ref {}", new Object[]{this.getCallingClass(), this.resource.getClass().getSimpleName(), System.identityHashCode(this.resource), this.refCnt.get()});
      }

   }

   public T getResource() {
      return this.resource;
   }

   public void close() throws Exception {
      if (this.refCnt.decrementAndGet() == 0 && this.resource != null) {
         logger.debug("Closing resource {}", this.resource.getClass().getSimpleName());
         this.closerFunc.accept(this.resource);
         this.resource = null;
      }

      if (logger.isDebugEnabled()) {
         logger.debug("Class {} released the ref for {}:{}, Current ref count:{}", new Object[]{this.getCallingClass(), this.resource.getClass().getSimpleName(), System.identityHashCode(this.resource), this.refCnt.get()});
      }

   }

   private String getCallingClass() {
      StackTraceElement traceElement = Thread.currentThread().getStackTrace()[3];
      return traceElement.getClassName() + ":" + traceElement.getMethodName() + ":" + traceElement.getLineNumber();
   }
}
