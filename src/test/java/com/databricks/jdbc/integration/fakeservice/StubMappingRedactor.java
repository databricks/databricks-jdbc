package com.databricks.jdbc.integration.fakeservice;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.STATEMENT_PATH;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.common.Json;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A custom {@link StubMappingTransformer} that redacts sensitive credentials in the stub mappings.
 */
public class StubMappingRedactor extends StubMappingTransformer {

  public static final String NAME = "stub-mapping-redactor";

  private static final String SERVER_HEADER_NAME = "Server";

  private static final String AMAZON_S3_SERVER_VALUE = "AmazonS3";

  private static final String AZURE_STORAGE_SERVER_VALUE = "Windows-Azure-Blob/1.0";

  /** Pattern to match sensitive credentials in the stub mappings. */
  private static final Pattern SENSITIVE_CREDS_PATTERN =
      Pattern.compile("(X-Amz-Security-Token|X-Amz-Credential|sig)=[^&\"]*[&\"]");

  /** {@inheritDoc} */
  @Override
  public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
    String requestUrl = stubMapping.getRequest().getUrl();
    String serverHeaderValue =
        stubMapping.getResponse().getHeaders().getHeader(SERVER_HEADER_NAME).firstValue();

    if (AMAZON_S3_SERVER_VALUE.equals(serverHeaderValue)
        || requestUrl.startsWith(STATEMENT_PATH)
        || serverHeaderValue.startsWith(AZURE_STORAGE_SERVER_VALUE)) {
      // Clean credentials from statement requests (embedded S3 links) and Amazon S3 responses.
      try {
        final String jsonString = getJsonStringFromStubMapping(stubMapping);
        final String transformedJsonString = redactCredentials(jsonString);
        return getStubMappingFromJsonString(transformedJsonString);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    } else {
      return stubMapping;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return NAME;
  }

  /** Removes sensitive credentials from the given JSON string. */
  private String redactCredentials(String jsonString) {
    Matcher matcher = SENSITIVE_CREDS_PATTERN.matcher(jsonString);
    StringBuilder buffer = new StringBuilder();

    while (matcher.find()) {
      matcher.appendReplacement(buffer, "[REDACTED]");
    }
    matcher.appendTail(buffer);

    return buffer.toString();
  }

  /** Returns the JSON string representation of the given {@link StubMapping}. */
  private String getJsonStringFromStubMapping(StubMapping stubMapping)
      throws JsonProcessingException {
    return JsonUtils.write(stubMapping, Json.PublicView.class);
  }

  /** Returns the {@link StubMapping} from the given JSON string. */
  private StubMapping getStubMappingFromJsonString(String jsonString)
      throws JsonProcessingException {
    return JsonUtils.read(jsonString, StubMapping.class);
  }
}
