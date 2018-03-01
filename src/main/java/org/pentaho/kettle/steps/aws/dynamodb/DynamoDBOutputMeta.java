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

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.aws.dynamodb.DynamoDBDatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.shared.SharedObjectInterface;
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
import org.pentaho.metastore.api.IMetaStore;

import org.w3c.dom.Node;

@Step( id = "DynamoDBOutput",
image = "DynamoDBOutput.png",
 i18nPackageName = "org.pentaho.kettle.steps.DynamoDBoutput", name = "DynamoDBOutput.Name",
 description = "DynamoDBOutput.Description",
 categoryDescription = "BaseStep.Category.BigData" )
/**
 * Metadata (configuration) holding class for the DynamoDB output step
 * 
 * @author afowler
 * @since 01-12-2017
 */
public class DynamoDBOutputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DynamoDBOutputMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * The connection to the database
   */
  private DatabaseMeta databaseMeta;

  private String tableField = "";


  @Override
  /**
   * Loads step configuration from PDI ktr file XML
   */
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
    readData(stepnode,databases);
  }

  @Override
  /**
   * Clones this meta class instance in PDI
   */
  public Object clone() {
    DynamoDBOutputMeta retval = (DynamoDBOutputMeta) super.clone();
    return retval;
  }
  
  /**
   * Sets default metadata configuration
   */
  public void setDefault() {
    databaseMeta = null;
  }

  @Override
  /**
   * Adds any additional fields to the stream
   */
  public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space) {
    // we don't add any, so leave blank
    // TODO add options for http status (success/failure) fields per document / row
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param database
   *          The database to set.
   */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /**
   * Actually read the XML stream (used by loadXML())
   */
  private void readData(Node entrynode, List<? extends SharedObjectInterface> databases) throws KettleXMLException {
    try {
      //host = XMLHandler.getTagValue(entrynode, "host");
      //port = Integer.parseInt(XMLHandler.getTagValue(entrynode, "port"));
      //username = XMLHandler.getTagValue(entrynode, "username");
      //password = XMLHandler.getTagValue(entrynode, "password");
      databaseMeta = DatabaseMeta.findDatabase(databases, XMLHandler.getTagValue(entrynode, "connection"));
      tableField = XMLHandler.getTagValue(entrynode, "table");
    } catch (Exception e) {
      throw new KettleXMLException(BaseMessages.getString(PKG, "DynamoDBOutputMeta.Exception.UnableToLoadStepInfo"), e);
    }
  }

  @Override
  /**
   * Returns the XML configuration of this step for saving in a ktr file
   */
  public String getXML() {
    StringBuffer retval = new StringBuffer(300);
    retval.append("    " + XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));

    retval.append("      ").append(XMLHandler.addTagValue("table", tableField));

    return retval.toString();
  }

  @Override
  /**
   * Reads the configuration of this step from a repository
   */
  public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
      throws KettleException {

    try {
      databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "id_connection", databases);
      tableField = rep.getJobEntryAttributeString(id_step, "table");
    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(PKG, "DynamoDBOutputMeta.Exception.UnexpectedErrorWhileReadingStepInfo"), e);
    }

  }

  @Override
  /**
   * Saves the configuration of this step to a repository
   */
  public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
      throws KettleException {
    try {
      rep.saveDatabaseMetaStepAttribute(id_transformation, id_step, "id_connection", databaseMeta);
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "table", tableField);
    } catch (KettleException e) {
      throw new KettleException(
          BaseMessages.getString(PKG, "DynamoDBOutputMeta.Exception.UnableToSaveStepInfo") + id_step, e);
    }
  }

  @Override
  /**
   * Validates this step's configuration
   */
  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore) {

    CheckResult cr;
    if (databaseMeta != null) {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Connection exists", stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Please select or create a connection to use",
          stepMeta);
      remarks.add(cr);
    }

    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
          BaseMessages.getString(PKG, "DynamoDBOutputMeta.CheckResult.StepReceiveInfo.DialogMessage"), stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString(PKG, "DynamoDBOutputMeta.CheckResult.NoInputReceived.DialogMessage"), stepMeta);
      remarks.add(cr);
    }

  }

  /**
   * Returns a new instance of this step
   */
  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans) {
    return new DynamoDBOutput(stepMeta, stepDataInterface, cnr, transMeta, trans);
  }

  /**
   * Returns a new instance of step data
   */
  public StepDataInterface getStepData() {
    return new DynamoDBOutputData();
  }
  
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] { databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

/*
  public void setHost(String h) {
    host = h;
  }

  public String getHost() {
    return host;
  }

  public void setPort(int p) {
    port = p;
  }

  public int getPort() {
    return port;
  }

  public void setUsername(String user) {
    username = user;
  }

  public String getUsername() {
    return username;
  }

  public void setPassword(String pass) {
    password = pass;
  }

  public String getPassword() {
    return password;
  }
*/
  public void setTableField(String tbl) {
    tableField = tbl;
  }

  public String getTableField() {
    return tableField;
  }

}