package com.dremio.plugins.util;

import java.io.IOException;

public class ContainerAccessDeniedException extends IOException {
   public ContainerAccessDeniedException(String message) {
      super(message);
   }

   public ContainerAccessDeniedException(String containerMsgKey, String containerName, Throwable cause) {
      super(String.format("Access to %s %s is denied - %s", containerMsgKey, containerName, cause.getMessage()), cause);
   }
}
