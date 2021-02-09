package com.dremio.plugins.util;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseableRef<T extends AutoCloseable> implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(CloseableRef.class);
   private AtomicInteger refCnt;
   private T obj;

   public CloseableRef(T obj) {
      if (logger.isDebugEnabled()) {
         logger.debug("Class {} acquired a new ref for {}:{}", new Object[]{this.getCallingClass(), obj.getClass().getSimpleName(), System.identityHashCode(obj)});
      }

      this.obj = obj;
      this.refCnt = new AtomicInteger(1);
   }

   public T acquireRef() {
      if (logger.isDebugEnabled()) {
         logger.debug("Class {} acquired the ref for {}:{}", new Object[]{this.getCallingClass(), this.obj.getClass().getSimpleName(), System.identityHashCode(this.obj)});
      }

      Preconditions.checkNotNull(this.obj);
      this.refCnt.incrementAndGet();
      return this.obj;
   }

   public void close() throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug("Class {} released the ref for {}:{}", new Object[]{this.getCallingClass(), this.obj.getClass().getSimpleName(), System.identityHashCode(this.obj)});
      }

      if (this.refCnt.decrementAndGet() == 0 && this.obj != null) {
         this.obj.close();
         this.obj = null;
      }

   }

   private String getCallingClass() {
      StackTraceElement traceElement = Thread.currentThread().getStackTrace()[3];
      return traceElement.getClassName() + ":" + traceElement.getMethodName() + ":" + traceElement.getLineNumber();
   }
}
