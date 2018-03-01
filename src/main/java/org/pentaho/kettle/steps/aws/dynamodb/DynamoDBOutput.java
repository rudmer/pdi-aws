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
import org.pentaho.di.core.database.aws.common.AWSAuthScheme;
import org.pentaho.di.core.database.aws.common.AWSDeploymentType;
import org.pentaho.di.core.database.aws.dynamodb.DynamoDBConnectionFactory;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

/**
 * DynamoDB Document Output Step
 *
 * @author Adam Fowler {@literal <adam.fowler@hitachivantara.com>}
 * @since 1.0 28-02-2018
 */
public class DynamoDBOutput extends BaseStep implements StepInterface {
  private static Class<?> PKG = DynamoDBOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private DynamoDBOutputMeta meta;
  private DynamoDBOutputData data;

  private boolean first = true;

  /**
   * Standard constructor
   */
  public DynamoDBOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /**
   * Processes a single row in the PDI stream
   */
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (DynamoDBOutputMeta) smi;
    data = (DynamoDBOutputData) sdi;

    Object[] r = getRow(); // get row, set busy!

    if ( null == r ) {
      logRowlevel("Processing last DynamoDB row");
      // no more input to be expected...

      // any connection cleanup
      completeBatch();

      logRowlevel("Processing last DynamoDB row completed");
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      logRowlevel("Processing first DynamoDB row");
      logRowlevel("Table Field: " + meta.getTableField());

      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, this.metaStore );

      String tableField = meta.getTableField();
      if (null != tableField) {
        data.tableFieldId = data.inputRowMeta.indexOfValue(tableField);
      }
      logDebug("tableField: " + tableField + " field id: " + data.tableFieldId);


      // create connection
      // TODO get connection to dynamodb
      try {

        //data.client = DynamoDBConnectionFactory.create(
        //  (String) meta.getDatabaseMeta().getAttributes().get(DynamoDBDatabaseMeta.ATTRIBUTE_HOST),
        //  Integer.parseInt((String) meta.getDatabaseMeta().getAttributes().get(DynamoDBDatabaseMeta.ATTRIBUTE_PORT)), 
        //  as,
        //  (String) meta.getDatabaseMeta().getAttributes().get(DynamoDBDatabaseMeta.ATTRIBUTE_USERNAME),
        //  (String) meta.getDatabaseMeta().getAttributes().get(DynamoDBDatabaseMeta.ATTRIBUTE_PASSWORD)
        //);
        data.client = ((DynamoDBDatabaseMeta)meta.getDatabaseMeta().getDatabaseInterface()).getConnection();
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "DynamoDBOutput.Log.CannotConnect"),e);
      }

      //data.client = ((DynamoDBDatabaseMeta)meta.getDatabaseMeta()).getConnection();


      logRowlevel("Processing first DynamoDB row completed");

    } // end if for first row (initialisation based on row data)

    // Do something to this row's data (create row for BigQuery, and append to current stream)
    data.inputRowMeta = getInputRowMeta();



    // create request
    String thisTableName = (String)r[data.tableFieldId];
    if (!data.lastTableName.equals(thisTableName) || data.recordsInBatch >= data.maxRecordsPerBatch) {
      // flush batch now, if records > 0
      completeBatch();
      data.recordsInBatch = 0; // reset counter
      // reset batch
      //data.lastTable = data.client.getTable(thisTableName);
      data.lastTableName = thisTableName;
      data.batcher = new TableWriteItems(thisTableName);
    }
    final Map<String, Object> infoMap = new HashMap<String, Object>();

    // get hold of all fields, other than tableField, and add to save request as a field of type (String by default)
    String[] names = data.inputRowMeta.getFieldNames();
    String pkName = "";
    Object pkValue = "";
    boolean first = true;
    for (int ni = 0;ni < names.length;ni++) {
      if (ni != data.tableFieldId) { // ignore the data item that holds this table's name
        String name = names[ni];
        Object value = r[data.inputRowMeta.indexOfValue(name)];
        logRowlevel("  Adding record key: " + name + " = " + value);
        if (first) {
          first = false;
          pkName = name;
          pkValue = value;
        }
        infoMap.put(name,value);
      }
    }
    try {
      // batched write code
      data.batcher.addItemToPut(new Item().withPrimaryKey(pkName, pkValue).withMap("info", infoMap));
      data.recordsInBatch++;
      // the following was for a single unbatched write
      /*
      PutItemOutcome outcome = table.putItem(new Item().withPrimaryKey(pkName, pkValue).withMap("info", infoMap));

      PutItemResult result = outcome.getPutItemResult();
      //logDebug(result);
      */
    } catch (Exception e) {
      logError("Error saving to DynamoDB", e);
    }
    
    // Also copy rows to output
    putRow( data.outputRowMeta, r );

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "DynamoDBOutput.Log.LineNumber" ) + getLinesRead( ) );
      }
    }

    return true;
  }

  private void completeBatch() {
    if (null != data.batcher && data.recordsInBatch > 0) {
      try {
        BatchWriteItemOutcome outcome = data.client.batchWriteItem(data.batcher);
    
        do {

          // Check for unprocessed keys which could happen if you exceed
          // provisioned throughput

          Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

          if (outcome.getUnprocessedItems().size() == 0) {
            System.out.println("No unprocessed items found");
          } else {
            System.out.println("Retrieving the unprocessed items");
            // retry
            outcome = data.client.batchWriteItemUnprocessed(unprocessedItems);
            Map<String, List<WriteRequest>> items = outcome.getUnprocessedItems();
            for (Map.Entry<String,String> httpEntry : 
              outcome.getBatchWriteItemResult().getSdkHttpMetadata().getHttpHeaders().entrySet()) {
              String header = httpEntry.getKey();
              String value = httpEntry.getValue();
              logRowlevel("HTTP response header '" + header + "' = '" + value + "'");
            }
            

            //now check

            for (Map.Entry<String, List<WriteRequest>> entry : items.entrySet()) {
              String tableName = entry.getKey();
              for (WriteRequest request : entry.getValue()) {
                Map<String, AttributeValue> key = request.getPutRequest().getItem();
                logError("Could not save record in table '" + tableName + "' with key '" + key + "'");
              }
            } // end for
          }

        } while (outcome.getUnprocessedItems().size() > 0);

      } catch (Exception e) {
        logError("Error saving to DynamoDB", e);
      }
    }
  }

  /**
   * Initialises the data for the step (meta data and runtime data)
   */
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    first = true;

    meta = (DynamoDBOutputMeta) smi;
    data = (DynamoDBOutputData) sdi;

    if ( super.init( smi, sdi ) ) {


      return true;
    }
    return false;
  }
}
