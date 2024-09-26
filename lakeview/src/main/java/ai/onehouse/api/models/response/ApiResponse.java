package ai.onehouse.api.models.response;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class ApiResponse {
  private boolean isFailure = false;
  private int statusCode = 200;
  private String cause = "";

  public void setError(int statusCode, String cause) {
    this.isFailure = true;
    this.statusCode = statusCode;
    this.cause = cause;
  }
}
