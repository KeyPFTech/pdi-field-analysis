/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.FieldAnalysis;
import org.pentaho.di.trans.steps.FieldAnalysisData;
import org.pentaho.di.trans.steps.FieldAnalysisMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.w3c.dom.Node;

/*
 * Created on 16-aug-2017
 *
 */

@Step( id = "FieldAnalysis0.2",
 image = "core/src/main/resources/PDI_MD_Profiler_V1.svg",
  i18nPackageName = "org.pentaho.di.sdk.samples.steps.demo", name = "FieldAnalysisMeta.Name",
  description = "FieldAnalysisMeta.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.Statistics" )

public class FieldAnalysisMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = FieldAnalysisMeta.class; // for i18n purposes, needed by Translator2!!

  public static final int TYPE_AGGREGATE_NONE = 0;
  public static final int TYPE_AGGREGATE_SUM = 1;
  public static final int TYPE_AGGREGATE_AVERAGE = 2;
  public static final int TYPE_AGGREGATE_COUNT = 3;
  public static final int TYPE_AGGREGATE_MIN = 4;
  public static final int TYPE_AGGREGATE_MAX = 5;
  public static final int TYPE_AGGREGATE_FIRST = 6;
  public static final int TYPE_AGGREGATE_LAST = 7;
  public static final int TYPE_AGGREGATE_FIRST_NULL = 8;
  public static final int TYPE_AGGREGATE_LAST_NULL = 9;

  public static final String[] aggregateTypeDesc = {
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.NONE" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.SUM" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.AVERAGE" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.COUNT" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.MIN" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.MAX" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.FIRST" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.LAST" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.FIRST_NULL" ),
    BaseMessages.getString( PKG, "FieldAnalysisMeta.AggregateTypeDesc.LAST_NULL" ), };

  private String[] fieldName;
  private String[] fieldNewName;
  private int[] aggregateType;

  public FieldAnalysisMeta() {
    super(); // allocate BaseStepMeta
  }






  /**
   * @return Returns the aggregateType.
   */
  public int[] getAggregateType() {
    return aggregateType;
  }

  /**
   * @param aggregateType
   *          The aggregateType to set.
   */
  public void setAggregateType( int[] aggregateType ) {
    this.aggregateType = aggregateType;
  }

  /**
   * @return Returns the fieldName.
   */
  public String[] getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName
   *          The fieldName to set.
   */
  public void setFieldName( String[] fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return Returns the fieldNewName.
   */
  public String[] getFieldNewName() {
    return fieldNewName;
  }

  /**
   * @param fieldNewName
   *          The fieldNewName to set.
   */
  public void setFieldNewName( String[] fieldNewName ) {
    this.fieldNewName = fieldNewName;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public void allocate( int nrfields ) {
    fieldName = new String[nrfields];
    fieldNewName = new String[nrfields];
    aggregateType = new int[nrfields];
  }

  public static final String getTypeDesc( int t ) {
    if ( t < 0 || t >= aggregateTypeDesc.length ) {
      return null;
    }
    return aggregateTypeDesc[t];
  }

  public static final int getType( String at ) {
    int i;
    for ( i = 0; i < aggregateTypeDesc.length; i++ ) {
      if ( aggregateTypeDesc[i].equalsIgnoreCase( at ) ) {
        return i;
      }
    }
    return TYPE_AGGREGATE_NONE;
  }

  @Override
  public Object clone() {
	  FieldAnalysisMeta retval = (FieldAnalysisMeta) super.clone();

    int nrfields = fieldName.length;

    retval.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      retval.fieldName[i] = fieldName[i];
      retval.fieldNewName[i] = fieldNewName[i];
      retval.aggregateType[i] = aggregateType[i];
    }
    return retval;
  }

  private void readData( Node stepnode ) throws KettleXMLException {
    try {
      int i, nrfields;
      String type;

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        fieldName[i] = XMLHandler.getTagValue( fnode, "name" );
        fieldNewName[i] = XMLHandler.getTagValue( fnode, "rename" );
        type = XMLHandler.getTagValue( fnode, "type" );
        aggregateType[i] = getType( type );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString(
        PKG, "FieldAnalysisMeta.Exception.UnableToLoadStepInfo" ), e );
    }
  }

  @Override
  public void setDefault() {
    int i, nrfields;

    nrfields = 0;

    allocate( nrfields );

    for ( i = 0; i < nrfields; i++ ) {
      fieldName[i] = BaseMessages.getString( PKG, "FieldAnalysisMeta.Fieldname.Label" );
      fieldNewName[i] = BaseMessages.getString( PKG, "FieldAnalysisMeta.NewName.Label" );
      aggregateType[i] = TYPE_AGGREGATE_SUM;
    }
  }

  @Override
  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    try {
      // Remember the types of the row.
      
      int[] fieldnrs = new int[fieldName.length];
      ValueMetaInterface[] values = new ValueMetaInterface[fieldName.length];

      for ( int i = 0; i < fieldName.length; i++ ) {
        fieldnrs[i] = r.indexOfValue( fieldName[i] );
        ValueMetaInterface v =r.getValueMeta( fieldnrs[i] );
        
        
        switch ( aggregateType[i] ) {
          case TYPE_AGGREGATE_AVERAGE:
          case TYPE_AGGREGATE_COUNT:
          case TYPE_AGGREGATE_SUM:
            values[i] = ValueMetaFactory.cloneValueMeta( v, ValueMetaInterface.TYPE_NUMBER );
            values[i].setLength( -1, -1 );
            break;
          default:
            values[i] = ValueMetaFactory.cloneValueMeta( v );
        }

        //row.removeValueMeta( fieldnrs[i] );
        
      }

      // Only the aggregate is returned!
      r.clear();
/*
      for ( int i = 0; i < fieldName.length; i++ ) {
        ValueMetaInterface v = values[i];
        v.setName( fieldNewName[i] );
        v.setOrigin( name );
        row.addValueMeta( v );
      }*/
      

      //data.outputRowMeta = new RowMeta();
      
      RowMeta row = new RowMeta();
      ValueMeta vm = new ValueMeta("FieldName",ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(name);
      row.addValueMeta(vm);

      vm = new ValueMeta("Type",ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      
      vm = new ValueMeta("Count",ValueMetaInterface.TYPE_INTEGER);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      
      vm = new ValueMeta("DistinctValuesCount",ValueMetaInterface.TYPE_INTEGER);
      vm.setOrigin(name);
      row.addValueMeta(vm);
            
      vm = new ValueMeta("NullValuesCount",ValueMetaInterface.TYPE_INTEGER);
      vm.setOrigin(name);
      row.addValueMeta(vm);

      vm = new ValueMeta("Min",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);

      vm = new ValueMeta("Max",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);

      vm = new ValueMeta("Sum",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);

      vm = new ValueMeta("Mean",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("Median",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("StandardDeviation",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("Skewness",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("IsBoolean",ValueMetaInterface.TYPE_BOOLEAN);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("DataType",ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("Format",ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("Dispersion",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("UniqueNames",ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("UniqueValues",ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      vm = new ValueMeta("MaxLength",ValueMetaInterface.TYPE_NUMBER);
      vm.setOrigin(name);
      row.addValueMeta(vm);
      
      r.addRowMeta(row);
      
      // WE MAY NEED TO STILL CALL REMOVE VALUE META FOR DELETED FIELDS
      /*
      for ( int i = 0; i < fieldName.length; i++ ) {
        int idx = r.indexOfValue( fieldName[i] );
        //ValueMetaInterface v = r.getValueMeta( idx);
        r.removeValueMeta(idx);
      }*/




    } catch ( Exception e ) {
      throw new KettleStepException( e );
    }
  }
 
  public void getFieldsFromInput( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
  VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
  try {
    // Remember the types of the row.
    int[] fieldnrs = new int[fieldName.length];
    ValueMetaInterface[] values = new ValueMetaInterface[fieldName.length];

    for ( int i = 0; i < fieldName.length; i++ ) {
      fieldnrs[i] = row.indexOfValue( fieldName[i] );
      ValueMetaInterface v = row.getValueMeta( fieldnrs[i] );
      switch ( aggregateType[i] ) {
        case TYPE_AGGREGATE_AVERAGE:
        case TYPE_AGGREGATE_COUNT:
        case TYPE_AGGREGATE_SUM:
          values[i] = ValueMetaFactory.cloneValueMeta( v, ValueMetaInterface.TYPE_NUMBER );
          values[i].setLength( -1, -1 );
          break;
        default:
          values[i] = ValueMetaFactory.cloneValueMeta( v );
      }
    }

    // Only the aggregate is returned!
    row.clear();

    for ( int i = 0; i < fieldName.length; i++ ) {
      ValueMetaInterface v = values[i];
      v.setName( fieldNewName[i] );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
  } catch ( Exception e ) {
    throw new KettleStepException( e );
  }
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 300 );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < fieldName.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", fieldName[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "rename", fieldNewName[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "type", getTypeDesc( aggregateType[i] ) ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {

    try {
      int nrfields = rep.countNrStepAttributes( id_step, "field_name" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        fieldName[i] = rep.getStepAttributeString( id_step, i, "field_name" );
        fieldNewName[i] = rep.getStepAttributeString( id_step, i, "field_rename" );
        aggregateType[i] = getType( rep.getStepAttributeString( id_step, i, "field_type" ) );
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "FieldAnalysisMeta.Exception.UnexpectedErrorWhileReadingStepInfo" ), e );
    }

  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
      for ( int i = 0; i < fieldName.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "field_name", fieldName[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_rename", fieldNewName[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_type", getTypeDesc( aggregateType[i] ) );
      }
    } catch ( KettleException e ) {
      throw new KettleException( BaseMessages.getString( PKG, "FieldAnalysisMeta.Exception.UnableToSaveStepInfo" )
        + id_step, e );
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {

    CheckResult cr;
    String message = "";

    if ( fieldName.length > 0 ) {
      boolean error_found = false;
      // See if all fields are available in the input stream...
      message =
        BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.FieldsNotFound.DialogMessage" ) + Const.CR;
      for ( int i = 0; i < fieldName.length; i++ ) {
        if ( prev.indexOfValue( fieldName[i] ) < 0 ) {
          message += "  " + fieldName[i] + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, message, stepMeta );
      } else {
        message = BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.AllFieldsOK.DialogMessage" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, message, stepMeta );
      }
      remarks.add( cr );

      // See which fields are dropped: comment on it!
      message =
        BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.IgnoredFields.DialogMessage" ) + Const.CR;
      error_found = false;

      for ( int i = 0; i < prev.size(); i++ ) {
        ValueMetaInterface v = prev.getValueMeta( i );
        boolean value_found = false;
        for ( int j = 0; j < fieldName.length && !value_found; j++ ) {
          if ( v.getName().equalsIgnoreCase( fieldName[j] ) ) {
            value_found = true;
          }
        }
        if ( !value_found ) {
          message += "  " + v.getName() + " (" + v.toStringMeta() + ")" + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        cr = new CheckResult( CheckResult.TYPE_RESULT_COMMENT, message, stepMeta );
      } else {
        message = BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.AllFieldsUsed.DialogMessage" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, message, stepMeta );
      }
      remarks.add( cr );
    } else {
      message = BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.NothingSpecified.DialogMessage" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, message, stepMeta );
      remarks.add( cr );
    }

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FieldAnalysisMeta.CheckResult.StepReceiveInfo.DialogMessage" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FieldAnalysisMeta.CheckResult.NoInputReceived.DialogMessage" ), stepMeta );
      remarks.add( cr );
    }

  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new FieldAnalysis( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new FieldAnalysisData();
  }
}
