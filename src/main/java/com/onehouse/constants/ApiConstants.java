package com.onehouse.constants;

public class ApiConstants {

  private ApiConstants() {}

  public static final String ONEHOUSE_API_ENDPOINT =
      System.getenv().getOrDefault("ONEHOUSE_API_ENDPOINT", "https://api.onehouse.ai");

  // API Endpoints
  public static final String INITIALIZE_TABLE_METRICS_CHECKPOINT =
      "/v1/community/initialize-tables";
  public static final String UPSERT_TABLE_METRICS_CHECKPOINT = "/v1/community/{0}/checkpoint";
  public static final String GET_TABLE_METRICS_CHECKPOINT = "/v1/community/checkpoints";
  public static final String GENERATE_COMMIT_METADATA_UPLOAD_URL = "/v1/community/{0}/upload-urls";

  // Header constants
  public static final String PROJECT_UID_KEY = "x-onehouse-project-uid";
  public static final String ONEHOUSE_API_KEY = "x-onehouse-api-key";
  public static final String ONEHOUSE_API_SECRET_KEY = "x-onehouse-api-secret";
  public static final String ONEHOUSE_REGION_KEY = "x-onehouse-region";
  public static final String ONEHOUSE_USER_UUID_KEY = "x-onehouse-uuid";
}
