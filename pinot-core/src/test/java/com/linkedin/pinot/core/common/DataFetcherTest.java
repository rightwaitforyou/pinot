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
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DataFetcherTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataFetcherTest.class);

  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + "DataFetchertest";
  private static final int NUM_ROWS = 1000;
  private static final String DIMENSION_NAME = "dimension";
  private static final String METRIC_NAME = "metric";
  private static final int MAX_STEP_LENGTH = 5;

  private final long _randomSeed = System.currentTimeMillis();
  private final Random _random = new Random(_randomSeed);
  private final String[] _dimensionValues = new String[NUM_ROWS];
  private final double[] _metricValues = new double[NUM_ROWS];
  private final GenericRow[] _segmentData = new GenericRow[NUM_ROWS];
  private DataFetcher _dataFetcher;

  @BeforeClass
  private void setup() throws Exception {
    // Generate random dimension and metric values.
    for (int i = 0; i < NUM_ROWS; i++) {
      double randomDouble = _random.nextDouble();
      String randomDoubleString = String.valueOf(randomDouble);
      HashMap<String, Object> map = new HashMap<>();
      map.put(DIMENSION_NAME, randomDoubleString);
      map.put(METRIC_NAME, randomDouble);
      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      _dimensionValues[i] = randomDoubleString;
      _metricValues[i] = randomDouble;
      _segmentData[i] = genericRow;
    }

    // Create an index segment with the random dimension and metric values.
    final Schema schema = new Schema();

    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(DIMENSION_NAME, FieldSpec.DataType.STRING, true);
    schema.addField(DIMENSION_NAME, dimensionFieldSpec);

    MetricFieldSpec metricFieldSpec = new MetricFieldSpec(METRIC_NAME, FieldSpec.DataType.DOUBLE);
    schema.addField(METRIC_NAME, metricFieldSpec);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(INDEX_DIR_PATH);
    config.setSegmentName("dataFetcherTestSegment");

    RecordReader reader = new RecordReader() {
      int index = 0;

      @Override
      public void init() throws Exception {
      }

      @Override
      public void rewind() throws Exception {
        index = 0;
      }

      @Override
      public boolean hasNext() {
        return index < NUM_ROWS;
      }

      @Override
      public Schema getSchema() {
        return schema;
      }

      @Override
      public GenericRow next() {
        return _segmentData[index++];
      }

      @Override
      public Map<String, MutableLong> getNullCountMap() {
        return null;
      }

      @Override
      public void close() throws Exception {
      }
    };

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, reader);
    driver.build();

    ReadMode mode = ReadMode.heap;
    IndexSegment indexSegment = Loaders.IndexSegment.load(new File(INDEX_DIR_PATH, driver.getSegmentName()), mode);

    // Get a data fetcher for the index segment.
    _dataFetcher = new DataFetcher(indexSegment);
  }

  @Test
  public void testGetValueArrayForColumn() {
    int[] docIdArray = new int[NUM_ROWS];
    int count = 0;
    for (int i = _random.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += _random.nextInt(MAX_STEP_LENGTH) + 1) {
      docIdArray[count++] = i;
    }
    _dataFetcher.initNewBlock(docIdArray, count);
    double[] valueArray = _dataFetcher.getValueArrayForColumn(METRIC_NAME);
    for (int i = 0; i < count; i++) {
      if (valueArray[i] != _metricValues[docIdArray[i]]) {
        LOGGER.error("For index {}, value does not match: fetched value: {}, expected value: {}", i, valueArray[i],
            _metricValues[docIdArray[i]]);
        LOGGER.error("Random Seed: {}", _randomSeed);
        Assert.fail();
      }
    }
  }

  @Test
  public void testGetHashCodeArrayForColumn() {
    int[] docIdArray = new int[NUM_ROWS];
    int count = 0;
    for (int i = _random.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += _random.nextInt(MAX_STEP_LENGTH) + 1) {
      docIdArray[count++] = i;
    }
    _dataFetcher.initNewBlock(docIdArray, count);
    double[] hashCodeArray = _dataFetcher.getHashCodeArrayForColumn(DIMENSION_NAME);
    for (int i = 0; i < count; i++) {
      if (hashCodeArray[i] != _dimensionValues[docIdArray[i]].hashCode()) {
        LOGGER.error("For index {}, hash code does not match: fetched value: {}, expected value: {}", i,
            hashCodeArray[i], _dimensionValues[docIdArray[i]].hashCode());
        LOGGER.error("Random Seed: {}", _randomSeed);
        Assert.fail();
      }
    }
  }
}
