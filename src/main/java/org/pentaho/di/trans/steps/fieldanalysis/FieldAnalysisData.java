/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.fieldanalysis;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.HashSet;

/**
 * @author afowler
 * @since 16-aug-2017
 *
 */
public class FieldAnalysisData extends BaseStepData implements StepDataInterface {

	public RowMetaInterface outputRowMeta;
	public RowMetaInterface inputRowMeta;

  public int[] fieldnrs;
  public int nrfields;

  public Object[] values;

  public Object[] fieldNames;

  // fieldName, type, then...
  public Object[] type; // continuous, categorical, time
  public long[] counts; // non nulls only counted
  public Object[] distinctValues; // HashSet 
  public Object[] allValues; // ArrayList<String> 
  public Object[] allNumericValues; // ArrayList<Double>
  public long[] nullCount;
  public Object[] min;
  public Object[] max;
  public Object[] sum; // to help calculate std dev and mean

  public Object[] mean;
  public Object[] median;
  public Object[] stddev;
  public Object[] skewness;

  public Object[] isBoolean; // Boolean values

  public Object[] uniqueNames; //String containing all unique values for a categorical variable
  public Object[] uniqueValues; //String containing all unique values for a categorical variable
  public Object[] maxLength;

  public FieldAnalysisData() {
    super();
  }


}
