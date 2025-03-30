package ai.onehouse.exceptions;

public class ObjectStorageClientException extends RuntimeException {
  public ObjectStorageClientException(Throwable cause) {
    super(cause);
  }

  public ObjectStorageClientException(String message) {
    super(message);
  }
}
