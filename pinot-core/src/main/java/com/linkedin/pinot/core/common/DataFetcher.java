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

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * DataFetcher is a higher level abstraction for data fetching. Given an index segment, DataFetcher can manage the
 * DataSource, Dictionary and BlockValSet for this segment, preventing redundant construction for these instances.
 * Initialize DataFetcher with block doc id set and block length, DataFetcher will take care of the data fetching
 * process and reuse resources if possible. DataFetcher can be used by both selection, aggregation and group-by data
 * fetching process, reducing duplicate codes and garbage collection.
 *
 * TODO: Support Multi Value Columns
 */
public class DataFetcher {
  private final IndexSegment _indexSegment;

  private final Map<String, DataSource> _columnToDataSourceMap = new HashMap<>();
  private final Map<String, Dictionary> _columnToDictionaryMap = new HashMap<>();
  private final Map<String, BlockValSet> _columnToBlockValSetMap = new HashMap<>();

  private int[] _blockDocIdArray;
  private int _blockLength;
  private final Set<String> _columnDictIdLoaded = new HashSet<>();
  private final Set<String> _columnValueLoaded = new HashSet<>();
  private final Set<String> _columnHashCodeLoaded = new HashSet<>();
  private final Map<String, int[]> _columnToDictIdArrayMap = new HashMap<>();
  private final Map<String, double[]> _columnToValueArrayMap = new HashMap<>();
  private final Map<String, double[]> _columnToHashCodeArrayMap = new HashMap<>();

  /**
   * Constructor for DataFetcher.
   *
   * @param indexSegment index segment.
   */
  public DataFetcher(IndexSegment indexSegment) {
    _indexSegment = indexSegment;
  }

  /**
   * Given a column, fetch its data source.
   *
   * @param column column name.
   * @return data source associated with this column.
   */
  public DataSource getDataSourceForColumn(String column) {
    DataSource dataSource = _columnToDataSourceMap.get(column);
    if (dataSource == null) {
      dataSource = _indexSegment.getDataSource(column);
      _columnToDataSourceMap.put(column, dataSource);
    }
    return dataSource;
  }

  /**
   * Given a column, fetch its dictionary.
   *
   * @param column column name.
   * @return dictionary associated with this column.
   */
  public Dictionary getDictionaryForColumn(String column) {
    Dictionary dictionary = _columnToDictionaryMap.get(column);
    if (dictionary == null) {
      dictionary = getDataSourceForColumn(column).getDictionary();
      _columnToDictionaryMap.put(column, dictionary);
    }
    return dictionary;
  }

  /**
   * Given a column, fetch its block value set.
   *
   * @param column column name.
   * @return block value set associated with this column.
   */
  public BlockValSet getBlockValSetForColumn(String column) {
    BlockValSet blockValSet = _columnToBlockValSetMap.get(column);
    if (blockValSet == null) {
      blockValSet = getDataSourceForColumn(column).getNextBlock().getBlockValueSet();
      _columnToBlockValSetMap.put(column, blockValSet);
    }
    return blockValSet;
  }

  /**
   * Init the data fetcher with a block doc id array and block length. This method should be called before fetching data
   * for any specific block.
   *
   * @param blockDocIdArray block doc id array.
   * @param blockLength length of the block.
   */
  public void initNewBlock(int[] blockDocIdArray, int blockLength) {
    _columnDictIdLoaded.clear();
    _columnValueLoaded.clear();
    _columnHashCodeLoaded.clear();

    _blockDocIdArray = blockDocIdArray;
    _blockLength = blockLength;
  }

  /**
   * Get dictionary id array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return dictionary id array associated with this column.
   */
  public int[] getDictIdArrayForColumn(String column) {
    int[] dictIdArray = _columnToDictIdArrayMap.get(column);
    if (!_columnDictIdLoaded.contains(column)) {
      if (dictIdArray == null) {
        dictIdArray = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToDictIdArrayMap.put(column, dictIdArray);
      }
      BlockValSet blockValSet = getBlockValSetForColumn(column);
      blockValSet.readIntValues(_blockDocIdArray, 0, _blockLength, dictIdArray, 0);
      _columnDictIdLoaded.add(column);
    }
    return dictIdArray;
  }

  /**
   * Get value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return value array associated with this column.
   */
  public double[] getValueArrayForColumn(String column) {
    double[] valueArray = _columnToValueArrayMap.get(column);
    if (!_columnValueLoaded.contains(column)) {
      if (valueArray == null) {
        valueArray = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToValueArrayMap.put(column, valueArray);
      }
      Dictionary dictionary = getDictionaryForColumn(column);
      int[] dictIdArray = getDictIdArrayForColumn(column);
      dictionary.readDoubleValues(dictIdArray, 0, _blockLength, valueArray, 0);
      _columnValueLoaded.add(column);
    }
    return valueArray;
  }

  /**
   * Get hash code array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return hash code array associated with this column.
   */
  public double[] getHashCodeArrayForColumn(String column) {
    double[] hashCodeArray = _columnToHashCodeArrayMap.get(column);
    if (!_columnHashCodeLoaded.contains(column)) {
      if (hashCodeArray == null) {
        hashCodeArray = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToHashCodeArrayMap.put(column, hashCodeArray);
      }
      Dictionary dictionary = getDictionaryForColumn(column);
      int[] dictIdArray = getDictIdArrayForColumn(column);
      for (int i = 0; i < _blockLength; i++) {
        int dictId = dictIdArray[i];
        if (dictId == Dictionary.NULL_VALUE_INDEX) {
          hashCodeArray[i] = Integer.MIN_VALUE;
        } else {
          hashCodeArray[i] = dictionary.get(dictId).hashCode();
        }
      }
      _columnHashCodeLoaded.add(column);
    }
    return hashCodeArray;
  }
}
