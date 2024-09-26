package com.onehouse.api.models.request;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Jacksonized
@Value
public class UploadedFile {
  private String name;
  private long lastModifiedAt;
}
