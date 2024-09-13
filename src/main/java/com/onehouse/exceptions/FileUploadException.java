package com.onehouse.exceptions;

import java.io.IOException;

public class FileUploadException extends RuntimeException {
  public FileUploadException(String message) {
    super(message);
  }

  public FileUploadException(IOException e) {
    super(e);
  }
}
