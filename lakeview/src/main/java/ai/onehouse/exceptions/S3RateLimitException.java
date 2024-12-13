package ai.onehouse.exceptions;

public class S3RateLimitException extends RuntimeException{
    public S3RateLimitException(String message) {
        super(message);
    }
}
