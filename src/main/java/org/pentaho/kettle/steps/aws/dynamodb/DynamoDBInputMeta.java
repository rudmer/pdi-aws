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
import org.pentaho.di.core.row.ValueMeta;
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

@Step(id = "DynamoDBInput", image = "DynamoDB_input.svg", i18nPackageName = "org.pentaho.kettle.steps.aws.dynamodb.DynamoDBInput", name = "DynamoDBInput.Name", description = "DynamoDBInput.Description", categoryDescription = "BaseStep.Category.BigData")
/**
 * Metadata (configuration) holding class for the DynamoDB input step
 * 
 * @author Adam Fowler {@literal <adam.fowler@hitachivantara.com>}
 * @since 1.0 28-02-2018
 */
public class DynamoDBInputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DynamoDBInputMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * The connection to the database
   */
  private DatabaseMeta databaseMeta;

  private String host = "localhost";
  private int port = 8000;
  private String username = "admin";
  private String password = "password";
  private String table = "";

  @Override
  /**
   * Loads step configuration from PDI ktr file XML
   */
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
    readData(stepnode, databases);
  }

  @Override
  /**
   * Clones this meta class instance in PDI
   */
  public Object clone() {
    DynamoDBInputMeta retval = (DynamoDBInputMeta) super.clone();
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
      VariableSpace space) throws KettleStepException {
    // we don't add any, so leave blank
    // TODO add options for http status (success/failure) fields per document / row
    //super.getFields(r,origin,info,nextStep,space);

    // copy over existing meta
    // set ALL output fields (E.g. Content) to have an origin of this step name
    //r.clear();
    //RowMeta row = new RowMeta();
    /*
    List<ValueMetaInterface> ml = r.getValueMetaList();
    //int idx = 0;
    for (ValueMetaInterface vmi: ml) {
      logRowlevel("Value Meta: " + vmi.getName());
      if (documentContentField.equals(vmi.getName())) { 
        // THIS IS OUR OUTPUT FIELD - WE MUST SET ITS ORIGIN TO THIS STEP
        ValueMeta vm = new ValueMeta(vmi.getName(),vmi.getType());
        vm.setOrigin(origin);
        row.addValueMeta(vm);
      } else {
        row.addValueMeta(vmi.clone());
      }
    }
    */
    
/*
    ValueMeta ok = new ValueMeta("OK",ValueMetaInterface.TYPE_STRING);
    ok.setOrigin(origin);
    row.addValueMeta(ok);
*/
    //if (!Utils.empty(documentContentField)) {
      /*
      ValueMeta vm;
      vm = new ValueMeta(documentUriField,ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(origin);
      row.addValueMeta(vm);
      vm = new ValueMeta(collection,ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(origin);
      row.addValueMeta(vm);
      vm = new ValueMeta(mimeTypeField,ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(origin);
      row.addValueMeta(vm);
      vm = new ValueMeta(formatField,ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(origin);
      row.addValueMeta(vm);
      vm = new ValueMeta(documentContentField,ValueMetaInterface.TYPE_STRING);
      vm.setOrigin(origin);
      row.addValueMeta(vm);
      //vm = new ValueMeta("Success",ValueMetaInterface.TYPE_BOOLEAN);
      //vm.setOrigin(origin);
      //row.addValueMeta(vm);
      */
    //}
    //r.clear();
    //r.addRowMeta(row);

    // We neither add or remove fields, and so we don't need to do anything here (CONFIRMED VIA EXTENSIVE TESTING)
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
      table = XMLHandler.getTagValue(entrynode, "table");
    } catch (Exception e) {
      throw new KettleXMLException(BaseMessages.getString(PKG, "DynamoDBInputMeta.Exception.UnableToLoadStepInfo"),
          e);
    }
  }

  @Override
  /**
   * Returns the XML configuration of this step for saving in a ktr file
   */
  public String getXML() {
    StringBuffer retval = new StringBuffer(300);
    retval.append("    " + XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));

    retval.append("      ").append(XMLHandler.addTagValue("table", table));

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
      table = rep.getJobEntryAttributeString(id_step, "table");
    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(PKG, "DynamoDBInputMeta.Exception.UnexpectedErrorWhileReadingStepInfo"), e);
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
      rep.saveJobEntryAttribute(id_transformation, getObjectId(), "table", table);
    } catch (KettleException e) {
      throw new KettleException(
          BaseMessages.getString(PKG, "DynamoDBInputMeta.Exception.UnableToSaveStepInfo") + id_step, e);
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
          BaseMessages.getString(PKG, "DynamoDBInputMeta.CheckResult.StepReceiveInfo.DialogMessage"), stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString(PKG, "DynamoDBInputMeta.CheckResult.NoInputReceived.DialogMessage"), stepMeta);
      remarks.add(cr);
    }

  }

  /**
   * Returns a new instance of this step
   */
  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans) {
    return new DynamoDBInput(stepMeta, stepDataInterface, cnr, transMeta, trans);
  }

  /**
   * Returns a new instance of step data
   */
  public StepDataInterface getStepData() {
    return new DynamoDBInputData();
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
  public void setTable(String tbl) {
    table = tbl;
  }

  public String getTable() {
    return table;
  }


}