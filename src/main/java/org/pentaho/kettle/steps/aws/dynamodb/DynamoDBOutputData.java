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

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;

import java.util.Hashtable;

/**
 * Runtime transient data container for the PDI BigQuery stream step
 * 
 * @author Adam Fowler {@literal <adam.fowler@hitachivantara.com>}
 * @since 1.0 28-02-2018
 */
public class DynamoDBOutputData extends BaseStepData implements StepDataInterface {
  public RowMetaInterface outputRowMeta;
  public RowMetaInterface inputRowMeta;

  // in flight configuration objects here (E.g. batch handler
  public DynamoDB client = null;
  //public DataMovementManager dmm = null;
  //public WriteBatcher batcher = null;

  public TableWriteItems batcher = null;
  //public Table lastTable = null;

  public int tableFieldId = -1;
  public int recordsInBatch = 0;
  public int maxRecordsPerBatch = 25; // TODO set this from somewhere else, not hardcoded - 25 max for AWS DynamoDB batch write
  public String lastTableName = "";

  public Hashtable<String,String> fieldTypes = new Hashtable<String,String>();
 
  /**
   * Default constructor
   */
  public DynamoDBOutputData() {
    super();
  }

}