package com.onehouse.api.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class ApiResponse {
  @JsonProperty("isFailure")
  private boolean isFailure = false;

  @JsonProperty("statusCode")
  private int statusCode = 200;

  @JsonProperty("cause")
  private String cause = "";

  public void setError(int statusCode, String cause) {
    this.isFailure = true;
    this.statusCode = statusCode;
    this.cause = cause;
  }
}
