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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;

import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Iterator;
import java.util.Set;

/**
 * Field Analysis
 *
 * @author afowler
 * @since 16-aug-2017
 */
public class FieldAnalysis extends BaseStep implements StepInterface {
  private static Class<?> PKG = FieldAnalysis.class; // for i18n purposes, needed by Translator2!!

  private FieldAnalysisMeta meta;
  private FieldAnalysisData data;

  private Pattern numberPattern;
  //private Matcher numberMatcher;

  private HashMap datePatternMap;

  public FieldAnalysis( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    String regex = "[\\x00-\\x20]*[+-]?(((((\\p{Digit}+)(\\.)?((\\p{Digit}+)?)([eE][+-]?(\\p{Digit}+))?)|(\\.((\\p{Digit}+))([eE][+-]?(\\p{Digit}+))?)|(((0[xX](\\p{XDigit}+)(\\.)?)|(0[xX](\\p{XDigit}+)?(\\.)(\\p{XDigit}+)))[pP][+-]?(\\p{Digit}+)))[fFdD]?))[\\x00-\\x20]*";
    numberPattern = Pattern.compile( regex ); // why do I need to compile? - SPEED!!!!!
    //numberMatcher = numberPattern.matcher("");

    datePatternMap = new HashMap<String, Pattern>();
    datePatternMap.put( "dd/MM/yy", Pattern.compile( "[0123]?[0-9]/(0?[1-9]|1[0-2])/[0-9][0-9]" ) );
    datePatternMap.put( "MM/dd/yy", Pattern.compile( "(0?[1-9]|1[0-2])/[0123]?[0-9]/[0-9][0-9]" ) );
    datePatternMap.put( "dd/MM/yyyy", Pattern.compile( "[0123]?[0-9]/(0?[1-9]|1[0-2])/[0-9][0-9][0-9][0-9]" ) );
    datePatternMap.put( "MM/dd/yyyy", Pattern.compile( "(0?[1-9]|1[0-2])/[0123]?[0-9]/[0-9][0-9][0-9][0-9]" ) );
    datePatternMap.put( "dd/MM/yyyy HH:mm", Pattern.compile( "[0123]?[0-9]/(0?[1-9]|1[0-2])/[0-9][0-9][0-9][0-9] ([0-1][0-9]|2[0-4]):[0-5][0-9]" ) );
    datePatternMap.put( "MM/dd/yyyy HH:mm", Pattern.compile( "(0?[1-9]|1[0-2])/[0123]?[0-9]/[0-9][0-9][0-9][0-9] ([0-1][0-9]|2[0-4]):[0-5][0-9]" ) );
  }

  private synchronized boolean isNumber( String val ) {
    //numberMatcher.reset(val); // RESET results in invalid matches (matching any number within a string, for example)
    Matcher newMatcher = numberPattern.matcher( val );
    return newMatcher.matches();
  }

  private synchronized void AddAggregate( RowMetaInterface rowMeta, Object[] r ) throws KettleValueException {
    for ( int i = 0; i < data.fieldnrs.length; i++ ) {
      ValueMetaInterface valueMeta = rowMeta.getValueMeta( data.fieldnrs[i] );
      Object valueData = r[data.fieldnrs[i]];


      if ( !valueMeta.isNull( valueData ) ) {
        data.counts[i]++; // only count non-zero values!

        // TODO handle non numerics

        // calculate our aggregates here
        String number = valueMeta.getString( valueData );

        if ( data.distinctValues[i] == null ) {
          data.distinctValues[i] = new HashSet<String>();
        }
        ((HashSet) data.distinctValues[i]).add( number ); // automatically removed duplicates
        if ( data.allValues[i] == null ) {
          data.allValues[i] = new ArrayList<String>();
        }
        ((ArrayList) data.allValues[i]).add( number );

        // if number...
        if ( isNumber( number ) && ((HashSet) data.distinctValues[i]).size() != 2 ) {
          //data.type[i] = "Continuous";
          double numberValue = Double.parseDouble( number );

          if ( data.allNumericValues[i] == null ) {
            data.allNumericValues[i] = new ArrayList<String>();
          }
          ((ArrayList) data.allNumericValues[i]).add( numberValue );

          if ( data.sum[i] == null ) {
            data.sum[i] = numberValue;
          } else {
            data.sum[i] = new Double( ( (Double) data.sum[i] ).doubleValue() + numberValue );
          }
          if ( data.min[i] == null ) {
            data.min[i] = numberValue;
          } else {
            if ( ( (Double) data.min[i] ) > numberValue ) {
              data.min[i] = numberValue;
            }
          }
          if ( data.max[i] == null ) {
            data.max[i] = numberValue;
          } else {
            if ( ( (Double) data.max[i] ) < numberValue ) {
              data.max[i] = numberValue;
            }
          }

        } /* else {
          // not a number...
          // either categorical or boolean - can't tell until al are complete (in buildAggregate) - so don't set anything here!
          //data.type[i] = "Categorical";



          // TODO date

        } // end if is number
        */
/*
        switch ( meta.getAggregateType()[i] ) {
          case FieldAnalysisMeta.TYPE_AGGREGATE_SUM:
          case FieldAnalysisMeta.TYPE_AGGREGATE_AVERAGE:
            Double number = valueMeta.getNumber( valueData );
            if ( data.values[i] == null ) {
              data.values[i] = number;
            } else {
              data.values[i] = new Double( ( (Double) data.values[i] ).doubleValue() + number.doubleValue() );
            }

            break;
          case FieldAnalysisMeta.TYPE_AGGREGATE_MIN:
            if ( data.values[i] == null ) {
              data.values[i] = valueData;
            } else {
              if ( valueMeta.compare( data.values[i], valueData ) < 0 ) {
                data.values[i] = valueData;
              }
            }

            break;
          case FieldAnalysisMeta.TYPE_AGGREGATE_MAX:
            if ( data.values[i] == null ) {
              data.values[i] = valueData;
            } lse {
              if ( valueMeta.compare( data.values[i], valueData ) > 0 ) {
                data.values[i] = valueData;
              }
            }

            break;
          case FieldAnalysisMeta.TYPE_AGGREGATE_NONE:
          case FieldAnalysisMeta.TYPE_AGGREGATE_FIRST:
            if ( data.values[i] == null ) {
              data.values[i] = valueData;
            }
            break;
          case FieldAnalysisMeta.TYPE_AGGREGATE_LAST:
            data.values[i] = valueData;
            break;
          default:
            break;
        }
      }
      */
/*
      switch ( meta.getAggregateType()[i] ) {
        case FieldAnalysisMeta.TYPE_AGGREGATE_FIRST_NULL: // First value, EVEN if it's NULL:
          if ( data.values[i] == null ) {
            data.values[i] = valueData;
          }
          break;
        case FieldAnalysisMeta.TYPE_AGGREGATE_LAST_NULL: // Last value, EVEN if it's NULL:
          data.values[i] = valueData;
          break;
        default:
          break;
      }
      */

      } else {
        data.nullCount[i]++;
      } // end null if
    }
  }

  // End of the road, build a row to output!
  private synchronized Object[] buildAggregates() {
    Object[] rows = new Object[data.inputRowMeta.size() ]; // number of fields
    // one row per field name
    // for each row, field name, count, sum, min, max, mean, distinctValues(count of)
    Object[] row;
    ArrayList<String> valueList;
    HashSet<String> distinctValues;
    Iterator<String> distinctValuesIter;
    long valueListSize;
    ArrayList<Double> allNumericValues;
    Double dval;
    Iterator<Double> dIter;
    long numNumeric;
    long notNumeric;
    long allNumericSize;
    double mean;
    double sigma;
    double skewnessSigma;
    double xMinusMean;
    double stddev;
    HashMap<String, Long> patternCounts;

    long largestCount;
    String largestFormat;
    String dateFormat;
    long myCount;

    Object key;
    Set<String> keySet;
    Iterator<String> keyIter;
    Pattern datePattern;
    long numMatches;

    HashMap<String, Integer> histogram;
    long maxLength = -1;

    String val;

    Matcher newMatcher;

    int NUM_SUM_FIELDS = 18;

    for ( int i = 0; i < data.fieldnrs.length; i++ ) {


      row = RowDataUtil.allocateRowData( NUM_SUM_FIELDS ); // summary fields
      rows[i] = row;

      // transpose fields to rows

      // calculate averages, stddev etc here
      data.mean[i] = new Double( ( (Double) data.sum[i] ).doubleValue() / ( (long) data.counts[i] ) );

      row[0] = data.fieldNames[i];
      // info for all types
      // Loop over all values to determine type
      valueList = (ArrayList) data.allValues[i];
      distinctValues = (HashSet<String>) data.distinctValues[i];

      maxLength  = -1;

      valueListSize = valueList.size();

      allNumericValues = (ArrayList<Double>) data.allNumericValues[i];
      numNumeric = allNumericValues.size();
      notNumeric = valueListSize - numNumeric;

      histogram = new HashMap<String, Integer>();
      /*
      for (String val: valueList ) {
        try {
          Double.parse(val);
          numNumeric++;
        } catch (Exception e) {
          // not a double
          notNumeric++;
        }
      }
      */

      if ( numNumeric > notNumeric && ( 2 != distinctValues.size() ) ) {
        // continuous numeric
        row[1] = "Continuous";
        row[13] = "Double"; // TODO sniff other numeric types (positiveInteger, etc)
        // continuous variable analysis
        row[5] = data.min[i];
        row[6] = data.max[i];
        row[7] = data.sum[i];
        row[8] = data.mean[i];
        // median, std dev, skewness
        // sort all values once
        Collections.sort( allNumericValues );
        allNumericSize = allNumericValues.size();
        if ( allNumericSize > 0 ) {
          row[9] = allNumericValues.get( (int) Math.floor( allNumericSize / 2 ) ); // median

          // now for std dev and skewness
          // skewness = [n / (n -1) (n - 2)] sum[(x_i - mean)^3] / std^3 
          mean = ( (Double) data.mean[i] ).doubleValue();
          sigma = 0.0d;
          skewnessSigma = 0.0d;
          xMinusMean = 0.0d;
          dIter = allNumericValues.iterator();
          while ( dIter.hasNext() ) {
            dval = dIter.next();
            xMinusMean = dval.doubleValue() - mean;
            sigma += Math.pow( xMinusMean, 2 );
            skewnessSigma += Math.pow( xMinusMean, 3 );
          }
          stddev = sigma / ( valueList.size() - 1 );
          row[10] = new Double( stddev ); // stddev
          if ( allNumericSize > 3 && 0 != stddev ) {
            // skewness
            row[11] = new Double(
              ( 1.0d * ( allNumericSize / ( allNumericSize - 1 ) * ( allNumericSize - 2 ) ) ) * skewnessSigma / Math.pow( stddev, 3 )
            );
          } // end skewness sanity if
        } // end has values if
        row[12] = Boolean.FALSE;
      } else {
        // categorical or boolean or date
        row[1] = "Categorical";



        //#######################################################################################################
        //
        //  Jonathan 
        //
        //#######################################################################################################
        //Go through whole list of values to count appearance of individual values

        //Intialize hashmap 
        Iterator<String> iter = distinctValues.iterator();
        while ( iter.hasNext() ) {
          histogram.put( iter.next(), 0 );
        }

        Iterator<String> valueIterator = valueList.iterator();

        while ( valueIterator.hasNext() ) {
          String i_key = valueIterator.next();
          Integer i_val = histogram.get( i_key ) + 1;
          histogram.put( i_key, i_val );
        }
       //#######################################################################################################

        // categorical variable analysis
        // TODO CHECK FOR DATE FORMATS HERE (may only be two dates mentioned - so need to check before boolean)
        // TODO performance check - check for hypehsn(1) and slashes if its a date, or : colons if a time

        patternCounts = new HashMap<String, Long>();

        distinctValuesIter = distinctValues.iterator();

        while ( distinctValuesIter.hasNext() ) {
          val = distinctValuesIter.next();

          // NB this check is here as a single check for any date pattern is quicker than a set of specific date patterns
          //    Overall execution will be slower if most fields are dates, faster otherwise
          // TODO make this more definitive, and a Pattern and Matcher. E.g. matches string with / or - for dates
          if ( val.contains( "/" ) ) {

            keySet = (Set<String>) datePatternMap.keySet();
            keyIter = keySet.iterator();
            while ( keyIter.hasNext() ) {
              key = keyIter.next();
              datePattern = (Pattern) datePatternMap.get( (String) key );
              numMatches = 0;
              newMatcher = datePattern.matcher( val );
              if ( newMatcher.matches() ) {
                numMatches++;
              }

              if ( numMatches > 0 ) { // no point in adding count if doesn't match!
                patternCounts.put( (String) key, numMatches );
              }
            } // end date pattern inner loop
          } // end if date boundary checker
        } // end distinct values loop

        // time series
        // min date
        // max date
        // frequency - daily, hourly, etc.
        // date format
        if ( 1 == patternCounts.size() ) { // no point evaluating size of counts, if only one!
          row[13] = "Date";
          row[1] = "TimeSeries";
          row[12] = Boolean.FALSE;
          row[14] = (String) patternCounts.keySet().iterator().next(); // we want the key, not the count

        } else if ( patternCounts.size() > 1 ) {
          // This happens an awful lot with dates in the low number of days and months!
          row[13] = "Date";
          row[1] = "TimeSeries";
          row[12] = Boolean.FALSE;
          //row[14] = "Unknown"; // placeholder
          largestCount = 0;
          largestFormat = "Unknown";
          keySet = (Set<String>) patternCounts.keySet();
          keyIter = keySet.iterator();
          while ( keyIter.hasNext() ) {
            key = keyIter.next();
            dateFormat = (String) key;
            myCount = patternCounts.get( dateFormat );
            if ( myCount > largestCount ) {
              largestCount = myCount;
              largestFormat = dateFormat;
            }
          }
          row[14] = largestFormat;
          // TODO store the count somewhere. Do we want to return multiples??? (comma delimited date formats and counts)

        } else {
          // 0 match - so not a date! Is it a boolean?

          // is Boolean?
          if ( 2 == distinctValues.size() ) {
            row[13] = "Boolean";
            row[12] = Boolean.TRUE;
          } else {
            row[13] = "String";
            row[12] = Boolean.FALSE;
          }
        } // end date if

        // max length returned? (string length)
        // number invalid count returned?
      }

      // summary data for all types
      row[2] = data.counts[i];
      row[3] = (long) distinctValues.size();
      row[4] = data.nullCount[i];

      // dispersion
      if ( 0 != distinctValues.size() ) {
        // zero distinct values (i.e. no values) should not result in a dispersion of 0
        // all values size must also be greater than 0
        row[15] = ( 1.0d * distinctValues.size() ) / ( 1.0d * valueList.size() ); // forcing double precision division, not integer
      }


      //#######################################################################################################
      //
      //  Jonathan 
      //  TODO: maxLength
      //#######################################################################################################
      if ( ( row[1] == "Categorical" ) && ( 0 != distinctValues.size() ) ) {

        String i_key;
        String uniqueNames = "";
        String uniqueVals = "";

        Iterator<String> keys = histogram.keySet().iterator();

        while ( keys.hasNext() ) {
          i_key = keys.next();
          uniqueNames += i_key + "|";
          uniqueVals +=  histogram.get( i_key ).toString() + "|";
          maxLength  = (long) Math.max( maxLength, (long) i_key.length() );
        }

        row[16] = uniqueNames.substring( 0, uniqueNames.length() - 1 );
        row[17] = uniqueVals.substring( 0, uniqueVals.length() - 1 );
        row[18] = maxLength;
      } else {
        row[16] = "";
        row[17] = "";
        row[18] = 0.0;
      }
    //#######################################################################################################

/*
      switch ( meta.getAggregateType()[i] ) {
        case FieldAnalysisMeta.TYPE_AGGREGATE_SUM:
        case FieldAnalysisMeta.TYPE_AGGREGATE_MIN:
        case FieldAnalysisMeta.TYPE_AGGREGATE_MAX:
        case FieldAnalysisMeta.TYPE_AGGREGATE_FIRST:
        case FieldAnalysisMeta.TYPE_AGGREGATE_LAST:
        case FieldAnalysisMeta.TYPE_AGGREGATE_NONE:
        case FieldAnalysisMeta.TYPE_AGGREGATE_FIRST_NULL: // First value, EVEN if it's NULL:
        case FieldAnalysisMeta.TYPE_AGGREGATE_LAST_NULL: // Last value, EVEN if it's NULL:
          agg[i] = data.values[i];
          break;
        case FieldAnalysisMeta.TYPE_AGGREGATE_COUNT:
          agg[i] = new Double( data.counts[i] );
          break;
        case FieldAnalysisMeta.TYPE_AGGREGATE_AVERAGE:
          agg[i] = new Double( ( (Double) data.values[i] ).doubleValue() / data.counts[i] );
          break;

        default:
          break;
      }
      */
    } // field nrs loop



    return rows;
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (FieldAnalysisMeta) smi;
    data = (FieldAnalysisData) sdi;

    Object[] r = getRow(); // get row, set busy!


    if ( r == null ) {
      // no more input to be expected...
      Object[] rows = buildAggregates(); // build a resume

      meta.getFields( getInputRowMeta(), getStepname(), null, null, this, repository, metaStore );
      Object[] row;
      for ( int rowNum = 0; rowNum < rows.length; rowNum++ ) {
        row = (Object[]) rows[rowNum];
        if ( null != row ) {
          putRow( data.outputRowMeta, row );
        } else {
          logError( "Unexpected field present in stream that is not configured for Field Analytics" );
          // THIS SHOULD NEVER HAPPEN!!!!!
          // This happens when a field is present in the stream, but not mentioned in this step's listed fields!!!
        }
      }
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      //data.outputRowMeta = getInputRowMeta().clone();
      data.inputRowMeta = getInputRowMeta();

      data.outputRowMeta = new RowMeta();
      data.outputRowMeta.addValueMeta( new ValueMeta( "FieldName", ValueMetaInterface.TYPE_STRING ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Type", ValueMetaInterface.TYPE_STRING ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Count", ValueMetaInterface.TYPE_INTEGER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "DistinctValuesCount", ValueMetaInterface.TYPE_INTEGER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "NullValuesCount", ValueMetaInterface.TYPE_INTEGER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Min", ValueMetaInterface.TYPE_NUMBER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Max", ValueMetaInterface.TYPE_NUMBER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Sum", ValueMetaInterface.TYPE_NUMBER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Mean", ValueMetaInterface.TYPE_NUMBER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Median", ValueMetaInterface.TYPE_NUMBER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "StandardDeviation", ValueMetaInterface.TYPE_NUMBER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Skewness", ValueMetaInterface.TYPE_NUMBER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "IsBoolean", ValueMetaInterface.TYPE_BOOLEAN ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "DataType", ValueMetaInterface.TYPE_STRING ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Format", ValueMetaInterface.TYPE_STRING ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "Dispersion", ValueMetaInterface.TYPE_NUMBER ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "UniqueNames", ValueMetaInterface.TYPE_STRING ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "UniqueValues", ValueMetaInterface.TYPE_STRING ) );
      data.outputRowMeta.addValueMeta( new ValueMeta( "MaxLength", ValueMetaInterface.TYPE_NUMBER ) );

      //Meta data creation based on first row

      data.nrfields = data.inputRowMeta.getFieldNames().length;
      data.fieldnrs = new int[ data.inputRowMeta.getFieldNames().length ];
      data.values = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.fieldNames = new Object[ data.inputRowMeta.getFieldNames().length ];

      // fieldName, type, then...
      data.type = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.counts = new long[ data.inputRowMeta.getFieldNames().length ];
      data.distinctValues = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.allValues = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.allNumericValues = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.nullCount = new long[ data.inputRowMeta.getFieldNames().length ];
      data.min = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.max = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.sum = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.mean = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.median = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.stddev = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.skewness = new Object[ data.inputRowMeta.getFieldNames().length ];
      data.isBoolean = new Object[ data.inputRowMeta.getFieldNames().length ];

//      data.uniqueNames= new Object[data.inputRowMeta.getFieldNames().length];
//      data.uniqueValues= new Object[data.inputRowMeta.getFieldNames().length];
//      data.maxLength= new Object[data.inputRowMeta.getFieldNames().length];
//      

      for ( int i = 0; i < data.inputRowMeta.getFieldNames().length; i++ ) {
        //for ( int i = 0; i < meta.getFieldName().length; i++ ) {
        String fieldName = data.inputRowMeta.getFieldNames()[i];

        //logError(fieldName);
        //logError(new Integer(data.inputRowMeta.indexOfValue( fieldName)).toString());
        //logError(data.fieldnrs.toString());

        data.fieldnrs[i] = data.inputRowMeta.indexOfValue( fieldName );
        data.fieldNames[i] = fieldName;
        if ( data.fieldnrs[i] < 0 ) {
          logError( BaseMessages.getString( PKG, "FieldAnalysis.Log.CouldNotFindField", data.inputRowMeta.getFieldNames()[i] ) );
          setErrors( 1 );
          stopAll();
          return false;
        }

        data.counts[i] = 0L;
        data.nullCount[i] = 0L;
        data.sum[i] = 0.0;
        data.distinctValues[i] = new HashSet<String>();
        data.allValues[i] = new ArrayList<String>();
        data.allNumericValues[i] = new ArrayList<Double>();
      }

    }

    AddAggregate( getInputRowMeta(), r );

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "FieldAnalysis.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (FieldAnalysisMeta) smi;
    data = (FieldAnalysisData) sdi;

    if ( super.init( smi, sdi ) ) {

//      int nrfields = meta.getFieldName().length;
//      data.fieldnrs = new int[nrfields];
//      data.values = new Object[nrfields];
//      data.counts = new long[nrfields];
//
//      data.fieldNames = new Object[nrfields];
//
//      data.type = new Object[nrfields];
//      data.max = new Object[nrfields];
//      data.min = new Object[nrfields];
//      data.distinctValues = new Object[nrfields];
//      data.allValues = new Object[nrfields];
//      data.allNumericValues = new Object[nrfields];
//      data.nullCount = new long[nrfields];
//      data.sum = new Object[nrfields];
//
//      data.mean = new Object[nrfields];
//      data.median = new Object[nrfields];
//      data.stddev = new Object[nrfields];
//      data.skewness = new Object[nrfields];
//      data.isBoolean = new Object[nrfields];

      return true;
    }
    return false;

  }

}
