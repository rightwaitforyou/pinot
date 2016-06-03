/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.segment.fetcher;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.FileUploadUtils;

public class HttpSegmentFetcher implements SegmentFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSegmentFetcher.class);
  private static final String MAX_RETRIES = "maxRetries";
  private static final int DEFAULT_MAX_RETRIES = 3;
  private int maxRetries = DEFAULT_MAX_RETRIES;

  @Override
  public void init(Map<String, String> configs) {
    if (configs.containsKey(MAX_RETRIES)) {
      try {
        maxRetries = Integer.parseInt(configs.get(MAX_RETRIES));
      } catch (Exception e) {
        maxRetries = DEFAULT_MAX_RETRIES;
      }
    }
  }

  @Override
  public void fetchSegmentToLocal(String uri, File tempFile) throws Exception {
    for (int retry = 1; retry <= maxRetries; ++retry) {
      try {
        final long httpGetResponseContentLength = FileUploadUtils.getFile(uri, tempFile);
        LOGGER.info("Downloaded file from {} to {}; Length of httpGetResponseContent: {}; Length of downloaded file: {}", uri, tempFile,
            httpGetResponseContentLength, tempFile.length());
        return;
      } catch (Exception e) {
        LOGGER.error("Failed to download file from {}, retry: {}", uri, retry, e);
        if (retry == maxRetries) {
          throw e;
        } else {
          long backOffTimeInSec = 5 * retry;
          Thread.sleep(backOffTimeInSec * 1000);
        }
      }
    }
  }
}
