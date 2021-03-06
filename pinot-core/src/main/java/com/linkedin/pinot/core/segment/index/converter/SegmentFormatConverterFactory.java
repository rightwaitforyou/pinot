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
package com.linkedin.pinot.core.segment.index.converter;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;

public class SegmentFormatConverterFactory {

  public static SegmentFormatConverter getConverter(SegmentVersion from, SegmentVersion to) {

    if (from.equals(SegmentVersion.v1) && to.equals(SegmentVersion.v2) ) {
      return new SegmentFormatConverterV1ToV2();
    }

    if ((from.equals(SegmentVersion.v1) || from.equals(SegmentVersion.v2)) &&
        to.equals(SegmentVersion.v3)) {
      return new SegmentV1V2ToV3FormatConverter();
    }

    throw new UnsupportedOperationException(
        "Unable to find a converter to convert segment from:" + from + " to:" + to);
  }
}
