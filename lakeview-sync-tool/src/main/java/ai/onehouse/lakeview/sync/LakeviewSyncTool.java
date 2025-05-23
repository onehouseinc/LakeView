package ai.onehouse.lakeview.sync;

import ai.onehouse.api.AsyncHttpClientWithRetry;
import ai.onehouse.api.OnehouseApiClient;
import ai.onehouse.config.Config;
import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.GCSConfig;
import ai.onehouse.config.models.common.OnehouseClientConfig;
import ai.onehouse.config.models.common.S3Config;
import ai.onehouse.config.models.configv1.ConfigV1;
import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.config.models.configv1.ParserConfig;
import ai.onehouse.metadata_extractor.ActiveTimelineInstantBatcher;
import ai.onehouse.metadata_extractor.HoodiePropertiesReader;
import ai.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import ai.onehouse.metadata_extractor.TableDiscoveryService;
import ai.onehouse.metadata_extractor.TableMetadataUploaderService;
import ai.onehouse.metadata_extractor.TimelineCommitInstantsUploader;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.metrics.Metrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.GCSAsyncStorageClient;
import ai.onehouse.storage.PresignedUrlFileUploader;
import ai.onehouse.storage.S3AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.providers.GcsClientProvider;
import ai.onehouse.storage.providers.S3AsyncClientProvider;
import com.beust.jcommander.JCommander;
import com.google.common.annotations.VisibleForTesting;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ai.onehouse.lakeview.sync.LakeviewSyncConfigHolder.BASE_PATH;
import static ai.onehouse.lakeview.sync.LakeviewSyncConfigHolder.LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS;
import static ai.onehouse.lakeview.sync.LakeviewSyncConfigHolder.LAKEVIEW_METADATA_EXTRACTOR_PATH_EXCLUSION_PATTERNS;

public class LakeviewSyncTool extends HoodieSyncTool implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LakeviewSyncTool.class);
  private static final Pattern LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS_PATTERN = Pattern.compile("([^.]+)\\.databases\\.([^.]+)\\.base_paths");

  private static final int HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS = 15;
  private static final int HTTP_CLIENT_MAX_RETRIES = 3;
  private static final long HTTP_CLIENT_RETRY_DELAY_MS = 1000;

  private final boolean isLakeviewSyncToolEnabled;
  @Nullable
  private final Config config;
  private final ExecutorService executorService;
  @Nullable
  private final TableDiscoveryAndUploadJob tableDiscoveryAndUploadJob;
  @Nullable
  private final AsyncHttpClientWithRetry asyncHttpClientWithRetry;
  private final int httpClientTimeoutSeconds;
  private final int httpClientMaxRetries;
  private final long httpClientRetryDelayMs;
  private final long timeoutInSeconds;

  public LakeviewSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    HoodieConfig hoodieConfig = new HoodieConfig(props);
    this.isLakeviewSyncToolEnabled = hoodieConfig.getBooleanOrDefault(LakeviewSyncConfigHolder.LAKEVIEW_SYNC_ENABLED);
    this.executorService = Executors.newFixedThreadPool(2);
    if (isLakeviewSyncToolEnabled) {
      this.config = getConfig(hoodieConfig);
      this.asyncHttpClientWithRetry = getAsyncHttpClientWithRetry(executorService);
      this.tableDiscoveryAndUploadJob = getTableDiscoveryAndUploadJob(this.config, this.executorService, this.asyncHttpClientWithRetry);
      this.httpClientTimeoutSeconds = hoodieConfig.getIntOrDefault(LakeviewSyncConfigHolder.LAKEVIEW_HTTP_CLIENT_TIMEOUT_SECONDS);
      this.httpClientMaxRetries = hoodieConfig.getIntOrDefault(LakeviewSyncConfigHolder.LAKEVIEW_HTTP_CLIENT_MAX_RETRIES);
      this.httpClientRetryDelayMs = Option.ofNullable(hoodieConfig.getLong(LakeviewSyncConfigHolder.LAKEVIEW_HTTP_CLIENT_RETRY_DELAY_MS)).orElse(Long.valueOf(LakeviewSyncConfigHolder.LAKEVIEW_HTTP_CLIENT_RETRY_DELAY_MS.defaultValue()));
      this.timeoutInSeconds = Option.ofNullable(hoodieConfig.getLong(LakeviewSyncConfigHolder.LAKEVIEW_SYNC_TOOL_TIMEOUT_SECONDS)).orElse(Long.valueOf(LakeviewSyncConfigHolder.LAKEVIEW_SYNC_TOOL_TIMEOUT_SECONDS.defaultValue()));
    } else {
      this.config = null;
      this.tableDiscoveryAndUploadJob = null;
      this.asyncHttpClientWithRetry = null;
      this.httpClientTimeoutSeconds = HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS;
      this.httpClientMaxRetries = HTTP_CLIENT_MAX_RETRIES;
      this.httpClientRetryDelayMs = HTTP_CLIENT_RETRY_DELAY_MS;
      this.timeoutInSeconds = -1;
    }
  }

  private Config getConfig(HoodieConfig hoodieConfig) {
    List<ParserConfig> parserConfigList = getParserConfig();
    AtomicReference<String> lakeNameRef = new AtomicReference<>();
    AtomicReference<String> databaseNameRef = new AtomicReference<>();
    String tableBasePath = hoodieConfig.getString(BASE_PATH);
    String finalTableBasePath;
    if (tableBasePath.startsWith("s3a://")) {
      finalTableBasePath = tableBasePath.replace("s3a://", "s3://");
    } else {
      finalTableBasePath = tableBasePath;
    }
    // identify the lake & database to which the current table base path belongs to
    parserConfigList
        .forEach(parserConfig -> parserConfig.getDatabases()
            .forEach(database -> {
              for (String basePath : database.getBasePaths()) {
                if (finalTableBasePath.startsWith(basePath) && lakeNameRef.get() == null) {
                  lakeNameRef.set(parserConfig.getLake());
                  databaseNameRef.set(database.getName());
                  break;
                }
              }
            }));
    if (lakeNameRef.get() != null) {
      ParserConfig parserConfig = ParserConfig.builder()
          .lake(lakeNameRef.get())
          .databases(Collections.singletonList(Database.builder()
              .name(databaseNameRef.get())
              .basePaths(Collections.singletonList(finalTableBasePath))
              .build()))
          .build();
      parserConfigList = Collections.singletonList(parserConfig);
    } else {
      throw new IllegalArgumentException("Couldn't find any lake/database associated with the current table in the configuration");
    }
    MetadataExtractorConfig metadataExtractorConfig = MetadataExtractorConfig.builder()
        .parserConfig(parserConfigList)
        .pathExclusionPatterns(getPathsToExclude(hoodieConfig))
        .jobRunMode(MetadataExtractorConfig.JobRunMode.ONCE)
        .build();
    OnehouseClientConfig onehouseClientConfig = OnehouseClientConfig.builder()
        .projectId(hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_PROJECT_ID))
        .apiKey(hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_API_KEY))
        .apiSecret(hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_API_SECRET))
        .userId(hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_USERID))
        .build();
    FileSystemConfiguration fileSystemConfiguration = getFileSystemConfiguration(hoodieConfig);
    return ConfigV1.builder()
        .version(hoodieConfig.getStringOrDefault(LakeviewSyncConfigHolder.LAKEVIEW_VERSION))
        .metadataExtractorConfig(metadataExtractorConfig)
        .onehouseClientConfig(onehouseClientConfig)
        .fileSystemConfiguration(fileSystemConfiguration)
        .build();
  }

  private FileSystemConfiguration getFileSystemConfiguration(HoodieConfig hoodieConfig) {
    FileSystemConfiguration.FileSystemConfigurationBuilder fileSystemConfigurationBuilder = FileSystemConfiguration.builder();
    Option<S3Config> s3Config = getS3Config(hoodieConfig);
    if (s3Config.isPresent()) {
      fileSystemConfigurationBuilder.s3Config(s3Config.get());
    } else {
      Option<GCSConfig> gcsConfig = getGCSConfig(hoodieConfig);
      if (gcsConfig.isPresent()) {
        fileSystemConfigurationBuilder.gcsConfig(gcsConfig.get());
      } else {
        String errorMessage = "Couldn't find any properties related to file system";
        LOG.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      }
    }
    return fileSystemConfigurationBuilder.build();
  }

  private Option<S3Config> getS3Config(HoodieConfig hoodieConfig) {
    String region = hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_S3_REGION);
    if (!StringUtils.isNullOrEmpty(region)) {
      return Option.of(S3Config.builder()
          .region(region)
          .accessKey(java.util.Optional.ofNullable(hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_S3_ACCESS_KEY)))
          .accessSecret(java.util.Optional.ofNullable(hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_S3_ACCESS_SECRET)))
          .build());
    } else {
      return Option.empty();
    }
  }

  private Option<GCSConfig> getGCSConfig(HoodieConfig hoodieConfig) {
    String gcsProjectId = hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_GCS_PROJECT_ID);
    if (!StringUtils.isNullOrEmpty(gcsProjectId)) {
      return Option.of(GCSConfig.builder()
          .projectId(java.util.Optional.of(gcsProjectId))
          .gcpServiceAccountKeyPath(java.util.Optional.ofNullable(hoodieConfig.getString(LakeviewSyncConfigHolder.LAKEVIEW_GCS_SERVICE_ACCOUNT_KEY_PATH)))
          .build());
    } else {
      return Option.empty();
    }
  }

  private java.util.Optional<List<String>> getPathsToExclude(HoodieConfig hoodieConfig) {
    String pathsToExclude = hoodieConfig.getStringOrDefault(LAKEVIEW_METADATA_EXTRACTOR_PATH_EXCLUSION_PATTERNS);
    if (StringUtils.isNullOrEmpty(pathsToExclude)) {
      return java.util.Optional.empty();
    } else {
      return java.util.Optional.of(Arrays.stream(pathsToExclude.split(","))
          .filter(entry -> !entry.isEmpty())
          .collect(Collectors.toList()));
    }
  }

  private List<ParserConfig> getParserConfig() {
    Map<String, ParserConfig> lakeNameToParserConfig = new HashMap<>();
    props.forEach((key, value) -> {
      if (key.toString().startsWith(LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS.key())) {
        String currentKey = key.toString();
        currentKey = currentKey.substring(LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS.key().length() + 1);
        Matcher matcher = LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS_PATTERN.matcher(currentKey);
        if (matcher.find()) {
          String lakeName = matcher.group(1);
          String databaseName = matcher.group(2);
          List<String> tableBasePaths = Arrays.asList(value.toString().split(","));

          ParserConfig currentParserConfig = lakeNameToParserConfig
              .computeIfAbsent(lakeName, lake -> ParserConfig.builder().lake(lake).databases(new ArrayList<>()).build());
          Database database = Database.builder().name(databaseName).basePaths(tableBasePaths).build();
          currentParserConfig.getDatabases().add(database);
        } else {
          LOG.warn("Couldn't parse lakes/databases from {}={}", key, value);
        }
      }
    });
    return new ArrayList<>(lakeNameToParserConfig.values());
  }

  private TableDiscoveryAndUploadJob getTableDiscoveryAndUploadJob(@Nonnull Config config,
                                                                   @Nonnull ExecutorService executorService,
                                                                   @Nonnull AsyncHttpClientWithRetry asyncHttpClientWithRetry) {
    StorageUtils storageUtils = new StorageUtils();
    AsyncStorageClient asyncStorageClient = getAsyncStorageClient(config, executorService, storageUtils);
    ConfigProvider configProvider = new ConfigProvider(config);

    LakeViewExtractorMetrics lakeViewExtractorMetrics = new LakeViewExtractorMetrics(Metrics.getInstance(),
        configProvider);

    TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(asyncStorageClient, storageUtils,
            configProvider, executorService, lakeViewExtractorMetrics);
    HoodiePropertiesReader hoodiePropertiesReader = new HoodiePropertiesReader(asyncStorageClient,
        lakeViewExtractorMetrics);
    OnehouseApiClient onehouseApiClient = new OnehouseApiClient(asyncHttpClientWithRetry, config,
        lakeViewExtractorMetrics);
    PresignedUrlFileUploader presignedUrlFileUploader = new PresignedUrlFileUploader(asyncStorageClient,
        asyncHttpClientWithRetry, lakeViewExtractorMetrics);
    TimelineCommitInstantsUploader timelineCommitInstantsUploader = new TimelineCommitInstantsUploader(asyncStorageClient,
        presignedUrlFileUploader, onehouseApiClient, storageUtils, executorService, new ActiveTimelineInstantBatcher(config),
        lakeViewExtractorMetrics, config);
    TableMetadataUploaderService tableMetadataUploaderService = new TableMetadataUploaderService(hoodiePropertiesReader,
        onehouseApiClient, timelineCommitInstantsUploader, lakeViewExtractorMetrics, executorService);

    return new TableDiscoveryAndUploadJob(tableDiscoveryService, tableMetadataUploaderService, lakeViewExtractorMetrics, asyncStorageClient);
  }

  private AsyncStorageClient getAsyncStorageClient(@Nonnull Config config, @Nonnull ExecutorService executorService,
                                                   StorageUtils storageUtils) {
    if (config.getFileSystemConfiguration().getS3Config() != null) {
      S3AsyncClientProvider s3AsyncClientProvider = new S3AsyncClientProvider(config, executorService);
      return new S3AsyncStorageClient(s3AsyncClientProvider, storageUtils, executorService);
    } else {
      GcsClientProvider gcsClientProvider = new GcsClientProvider(config);
      return new GCSAsyncStorageClient(gcsClientProvider, storageUtils, executorService);
    }
  }

  private AsyncHttpClientWithRetry getAsyncHttpClientWithRetry(@Nonnull ExecutorService executorService) {
    Dispatcher dispatcher = new Dispatcher(executorService);
    OkHttpClient okHttpClient = new OkHttpClient.Builder()
        .readTimeout(httpClientTimeoutSeconds, TimeUnit.SECONDS)
        .writeTimeout(httpClientTimeoutSeconds, TimeUnit.SECONDS)
        .connectTimeout(httpClientTimeoutSeconds, TimeUnit.SECONDS)
        .dispatcher(dispatcher)
        .build();
    return new AsyncHttpClientWithRetry(
        httpClientMaxRetries, httpClientRetryDelayMs, okHttpClient);
  }

  @VisibleForTesting
  public @Nullable Config getConfig() {
    return config;
  }

  @Override
  public void syncHoodieTable() {
    if (isLakeviewSyncToolEnabled && tableDiscoveryAndUploadJob != null) {
      Future<?> future = executorService.submit(() -> tableDiscoveryAndUploadJob.runOnce());
      try {
        if (timeoutInSeconds > 0) {
          future.get(timeoutInSeconds, TimeUnit.SECONDS);
        } else {
          future.get();
        }
      } catch (TimeoutException e) {
        LOG.error("Lakeview sync operation got timed out", e);
        future.cancel(true);
      } catch (Exception e) {
        LOG.error("Failed to perform sync operation in lakeview", e);
      }
    }
  }

  @Override
  public void close() {
    try {
      super.close();
      if (executorService != null) {
        executorService.shutdown();
      }
      if (tableDiscoveryAndUploadJob != null) {
        tableDiscoveryAndUploadJob.shutdown();
      }
      if (asyncHttpClientWithRetry != null) {
        asyncHttpClientWithRetry.shutdownScheduler();
      }
    } catch (Exception e) {
      LOG.error("Failed to close lakeview sync tool", e);
    }
  }

  public static void main(String[] args) {
    final LakeviewSyncConfigHolder.LakeviewSyncConfigParams params = new LakeviewSyncConfigHolder.LakeviewSyncConfigParams();
    JCommander cmd = JCommander.newBuilder()
        .addObject(params)
        .build();
    cmd.parse(args);
    if (params.isHelp()) {
      cmd.usage();
    } else {
      try (LakeviewSyncTool lakeviewSyncTool = new LakeviewSyncTool(params.toProps(), new Configuration())) {
        lakeviewSyncTool.syncHoodieTable();
      }
    }
  }
}

