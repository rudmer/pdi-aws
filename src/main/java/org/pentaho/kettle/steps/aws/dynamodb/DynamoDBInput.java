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

package org.pentaho.kettle.steps.aws.dynamodb;

import org.pentaho.di.core.database.aws.dynamodb.DynamoDBDatabaseMeta;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Hashtable;

import java.nio.ByteBuffer;

/**
 * DynamoDB Document Input Step
 *
 * @author Adam Fowler {@literal <adam.fowler@hitachivantara.com>}
 * @since 1.0 28-02-2018
 */
public class DynamoDBInput extends BaseStep implements StepInterface {
  private static Class<?> PKG = DynamoDBInputMeta.class; // for i18n purposes, needed by Translator2!!

  private DynamoDBInputMeta meta;
  private DynamoDBInputData data;

  //private boolean first = true;

  /**
   * Standard constructor
   */
  public DynamoDBInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);

    meta = (DynamoDBInputMeta) getStepMeta().getStepMetaInterface();
    data = (DynamoDBInputData) stepDataInterface;
  }

  /**
   * Processes a single row in the PDI stream
   */
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    meta = (DynamoDBInputMeta) smi;
    data = (DynamoDBInputData) sdi;

    Object[] r = getRow(); // get row, set busy!

    if (null == r) {
      logRowlevel("Processing last DynamoDB row");

      logRowlevel("Processing last DynamoDB row completed");
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      logRowlevel("Processing first DynamoDB row");
      logRowlevel("Table: " + meta.getTable()); 

      data.inputRowMeta = getInputRowMeta();//.clone();
      //data.outputRowMeta = getInputRowMeta().clone();
      //data.outputRowMeta = new RowMeta();
      //Class c = data.inputRowMeta.getClass();
      //logDebug("input row meta class: " + c.getName());
      //c = data.outputRowMeta.getClass();
      //logDebug("output row meta clone class: " + c.getName());
      meta.getFields( getInputRowMeta(), getStepname(), null, null, this, repository, metaStore );
      data.outputRowMeta = getInputRowMeta().clone(); // modified by previous step (getFields), so MUST be called AFTER it

      // copy field definitions over from input to output (as per field analysis plugin)

      //data.outputRowMeta = new RowMeta();
      /*
      RowMetaInterface irm = getInputRowMeta();
      List<ValueMetaInterface> ml = irm.getValueMetaList();
      for (ValueMetaInterface vmi: ml) {
        logRowlevel("Value Meta: " + vmi.getName());
        data.outputRowMeta.addValueMeta(vmi);
      }
      */
      /*
      data.outputRowMeta.addValueMeta(new ValueMeta( "Collection", ValueMetaInterface.TYPE_STRING ));
      data.outputRowMeta.addValueMeta(new ValueMeta( "Content", ValueMetaInterface.TYPE_STRING ));
      data.outputRowMeta.addValueMeta(new ValueMeta( "MimeType", ValueMetaInterface.TYPE_STRING ));
      data.outputRowMeta.addValueMeta(new ValueMeta( "Format", ValueMetaInterface.TYPE_STRING ));
      data.outputRowMeta.addValueMeta(new ValueMeta( "Uri", ValueMetaInterface.TYPE_STRING ));
      data.outputRowMeta.addValueMeta(new ValueMeta( "OK", ValueMetaInterface.TYPE_STRING ));
      */
      

      // get IDs of fields we require
      String tableField = meta.getTable();
      if (null != tableField) {
        data.tableFieldId = data.inputRowMeta.indexOfValue(tableField);
      }

      // create connection
      // get connection to dynamodb
      try {
        data.client = ((DynamoDBDatabaseMeta) meta.getDatabaseMeta().getDatabaseInterface()).getConnection();
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "DynamoDBOutput.Log.CannotConnect"), e);
      }

      // create some sort of batching manager here, or top level class used to load documents
      
      String thisTableName = (String) r[data.tableFieldId];

      // loop through field names present, except table field, and create projection expression
      String fieldNamesExpression = "info, ";
      String[] names = data.inputRowMeta.getFieldNames();
      Hashtable<String,String> fieldTypes = new Hashtable<String,String>();

      boolean first = true;
      for (int ni = 0;ni < names.length;ni++) {
        if (ni != data.tableFieldId) { // ignore the data item that holds this table's name
          String name = names[ni];
          if (first) {
            first = false;
          } else {
            fieldNamesExpression += ", ";
          }
          fieldNamesExpression += name;
          // now determine the type and cache the answer in fieldTypes
          String typeDesc = data.inputRowMeta.getValueMeta(data.inputRowMeta.indexOfValue(name)).getTypeDesc();
          /*if ("String".equals(typeDesc)) {
            logRowlevel("Got String: " + name);
          } else {
            logRowlevel("Got Other: " + name + " of type " + typeDesc);
          }*/
          fieldTypes.put(name,typeDesc);
        } // endif
      } // endfor
      

      Map<String, AttributeValue> lastKeyEvaluated = null;
      do {
        ScanRequest scanRequest = new ScanRequest().withTableName(thisTableName).withLimit(25)
            .withExclusiveStartKey(lastKeyEvaluated).withProjectionExpression(fieldNamesExpression);

        Object[] outputRowData;
        ScanResult result = data.client.scan(scanRequest);
        for (Map<String, AttributeValue> item : result.getItems()) {
          // extract keys and place in to a new row in PDI
          outputRowData = RowDataUtil.createResizedCopy(r, data.outputRowMeta.size()); //.outputRowMeta.size());
          for (String key : item.keySet()) { 
            int fieldId = data.inputRowMeta.indexOfValue(key);
            if (-1 != fieldId) {
              // switch on type of field to determine what to get from dynamodb
              String type = fieldTypes.get(key);
              if ("String".equals(type)) {
                outputRowData[fieldId] = item.get(key).getS(); 
              } else if ("Number".equals(type)) {
                outputRowData[fieldId] = new Double(Double.parseDouble(item.get(key).getN()));
              } else if ("Integer".equals(type)) {
                outputRowData[fieldId] = new Long(Long.parseLong(item.get(key).getN()));
              } else if ("Boolean".equals(type)) {
                outputRowData[fieldId] = item.get(key).getBOOL();
              } else if ("Binary".equals(type)) {
                ByteBuffer buffer = item.get(key).getB();
                byte[] b = new byte[buffer.remaining()];
                buffer.get(b);
                outputRowData[fieldId] = b;
              } else {
                // UH OH!!! Unknown type
              }
            }
          } // end keyset for
          for (Map.Entry<String,AttributeValue> entry : item.entrySet()) {
            String entryName = entry.getKey();
            AttributeValue value = entry.getValue();
            logRowlevel("Entry '" + entry + "' has value: " + value);
          }

          // put row
          try {
            putRow(data.outputRowMeta, outputRowData);
          } catch (KettleStepException kse) {
            logError(BaseMessages.getString(PKG, "DynamoDBInput.Log.ExceptionPuttingRow"), kse);
          }

        }
        lastKeyEvaluated = result.getLastEvaluatedKey();
      } while (lastKeyEvaluated != null);

/*
      data.dmm = data.client.newDataMovementManager();

      StructuredQueryBuilder qb = data.client.newQueryManager().newStructuredQueryBuilder();

      data.batcher = data.dmm.newQueryBatcher(
        qb.collection((String) r[data.inputRowMeta.indexOfValue(meta.getCollection())] ) 
      );

      data.batcher.withConsistentSnapshot().withBatchSize(100).withThreadCount(3).onUrisReady(
        new ExportListener().onDocumentReady(doc -> {
          //logRowlevel("Retrieving DynamoDB Document: " + doc.getUri());
          // 1. Create new results row for output
          Object[] outputRowData;
          

          outputRowData = RowDataUtil.createResizedCopy(r, data.outputRowMeta.size() ); //.outputRowMeta.size());
          //outputRowData = r;

          //logDebug("r row data num fields: " + r.length);
          //logDebug("output row data num fields: " + outputRowData.length);

          // 2. Fill data in new result row
          String uri = doc.getUri();
          if (-1 != data.docUriFieldId) {
            outputRowData[data.docUriFieldId] = uri;
          }
          //String collection = (String)r[data.collectionFieldId];
          //if (-1 != data.collectionFieldId) {
            //outputRowData[data.collectionFieldId] = collection;
          //}
          if (-1 != data.mimeTypeFieldId) {
            outputRowData[data.mimeTypeFieldId] = (String)doc.getMimetype();
          }
          if (-1 != data.formatFieldId) {
            outputRowData[data.formatFieldId] = (String)doc.getFormat().toString();
          }
          //String uriParts[] = doc.getUri().split("/");
          // check that content was requested (not just URIs or metadata)
          if (-1 != data.docContentFieldId) {
            try {
              StringHandle content = new StringHandle();
              doc.getContent(content);
              //Files.write(Paths.get(EX_DIR, "output", uriParts[uriParts.length - 1]),
              //  doc.getContent(new StringHandle()).toBuffer());
  
              // output content to content field
              outputRowData[data.docContentFieldId] = content.toString();

            } catch (Exception e) {
              e.printStackTrace();
            }
          } // end if content requested if

          //outputRowData[5] = Boolean.TRUE;

          // 3. putRow
          try {
            putRow(data.outputRowMeta, outputRowData);
          } catch (KettleStepException kse) {
            logError(BaseMessages.getString(PKG, "DynamoDBInput.Log.ExceptionPuttingRow"), kse);
          }
        })
      ).onQueryFailure(exception -> exception.printStackTrace());

      // start the job and feed input to the batcher
      data.dmm.startJob(data.batcher);
      logRowlevel("Processing first DynamoDB query row completed");
*/
    } // end if for first row (initialisation based on row data)



    // Execute request and then create a row for each document



    // Don't output incoming row - done for us in the first row invocation
    // putRow(data.outputRowMeta, r);

    if (checkFeedback(getLinesRead())) {
      if (log.isBasic()) {
        logBasic(BaseMessages.getString(PKG, "DynamoDBInput.Log.LineNumber") + getLinesRead());
      }
    }

    return true;
  }

  /**
   * Initialises the data for the step (meta data and runtime data)
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (DynamoDBInputMeta) smi;
    data = (DynamoDBInputData) sdi;

    if (super.init(smi, sdi)) {

      return true;
    }
    return false;
  }
  
  @Override
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    //meta = (DynamoDBInputMeta) smi;
    //data = (DynamoDBInputData) sdi;

    //data.outputRowData = null;
    data.outputRowMeta = null;
    data.inputRowMeta = null;
    data.client = null;
    data.tableFieldId = -1;

    logRowlevel("dispose called on DynamoDBInput step");

    super.dispose(smi, sdi);
  }
  
}
