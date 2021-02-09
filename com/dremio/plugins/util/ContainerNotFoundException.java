package com.dremio.plugins.util;

import java.io.IOException;

public class ContainerNotFoundException extends IOException {
   public ContainerNotFoundException() {
   }

   public ContainerNotFoundException(String message) {
      super(message);
   }

   public ContainerNotFoundException(String message, Throwable cause) {
      super(message, cause);
   }

   public ContainerNotFoundException(Throwable cause) {
      super(cause);
   }
}
