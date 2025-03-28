package ai.onehouse.constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
  public static final String LINK_UID_KEY = "x-onehouse-link-uid";
  public static final String ONEHOUSE_REGION_KEY = "x-onehouse-region";
  public static final String ONEHOUSE_USER_UUID_KEY = "x-onehouse-uuid";
  public static final String MAINTENANCE_MODE_KEY = "x-maintenance-mode";
  // using mapping from:
  // https://chromium.googlesource.com/external/github.com/grpc/grpc/+/refs/tags/v1.21.4-pre1/doc/statuscodes.md
  public static final List<Integer> ACCEPTABLE_HTTP_FAILURE_STATUS_CODES =
      Collections.unmodifiableList(new ArrayList<>(Arrays.asList(404, 400, 403, 401, 409)));

  public static final String UNAUTHORIZED_ERROR_MESSAGE =
      "Confirm that your API token is valid and has not expired.";
}
