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

import org.pentaho.di.core.database.DatabaseMeta;

import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.List;
import java.util.ArrayList;

/**
 * Dialog box for the DynamoDB input step
 * 
 * @author Adam Fowler {@literal <adam.fowler@hitachivantara.com>}
 * @since 1.0 28-02-2018
 */
public class DynamoDBInputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = DynamoDBInputMeta.class; // for i18n purposes, needed by Translator2!!

  private DynamoDBInputMeta input;

  private Label wlName;
  private Text wName;

  private FormData fdlName, fdName;

  private CCombo wConnection;
  /*
  private Label cplName;
  private Text cpName;
  private FormData fcplName, fcpName;
  
  private Label plName;
  private Text pName;
  private FormData fplName, fpName;
  
  private Label dslName;
  private Text dsName;
  private FormData fdslName, fdsName;
  
  private Label tlName;
  private Text tName;
  private FormData ftlName, ftName;
  */

  private Label lblTable;
  private Text table;
  private FormData flblTable, fTable;


  /*
  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields,fdFields;
  */

  private Button wOK, wCancel;

  private Listener lsOK, lsCancel;

  private SelectionAdapter lsDef;

  private boolean changed = false;

  /**
   * Standard PDI dialog constructor
   */
  public DynamoDBInputDialog(Shell parent, Object in, TransMeta tr, String sname) {
    super(parent, (BaseStepMeta) in, tr, sname);
    input = (DynamoDBInputMeta) in;
  }

  /**
   * Initialises and displays the dialog box
   */
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    ModifyListener lsConnectionMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    };

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "DynamoDBInput.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step Name
    wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "DynamoDBInput.Name.Label"));
    props.setLook(wlName);
    fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wName.setToolTipText(BaseMessages.getString(PKG, "DynamoDBInput.Name.Tooltip"));
    props.setLook(wName);
    wName.addModifyListener(lsMod);
    fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Database Connection

    wConnection = addConnectionLine(shell, wName, middle, margin);
    List<String> items = new ArrayList<String>();
    for (DatabaseMeta dbMeta : transMeta.getDatabases()) {
      if (dbMeta.getDatabaseInterface() instanceof DynamoDBDatabaseMeta) {
        items.add(dbMeta.getName());
      }
    }
    wConnection.setItems(items.toArray(new String[items.size()]));
    if (input.getDatabaseMeta() == null && transMeta.nrDatabases() == 1) {
      wConnection.select(0);
    }
    wConnection.addModifyListener(lsConnectionMod);

    /*
    // Hostname
    cplName = new Label(shell, SWT.RIGHT);
    cplName.setText(BaseMessages.getString(PKG, "DynamoDBInput.Hostname.Label"));
    props.setLook(cplName);
    fcplName = new FormData();
    fcplName.left = new FormAttachment(0, 0);
    fcplName.right = new FormAttachment(middle, -margin);
    fcplName.top = new FormAttachment(wName, margin);
    cplName.setLayoutData(fcplName);
    cpName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    cpName.setToolTipText(BaseMessages.getString(PKG, "DynamoDBInput.Hostname.Tooltip"));
    props.setLook(cpName);
    cpName.addModifyListener(lsMod);
    fcpName = new FormData();
    fcpName.left = new FormAttachment(middle, 0);
    fcpName.top = new FormAttachment(wName, margin);
    fcpName.right = new FormAttachment(100, 0);
    cpName.setLayoutData(fcpName);
    
    // port
    plName = new Label(shell, SWT.RIGHT);
    plName.setText(BaseMessages.getString(PKG, "DynamoDBInput.Port.Label"));
    props.setLook(plName);
    fplName = new FormData();
    fplName.left = new FormAttachment(0, 0);
    fplName.right = new FormAttachment(middle, -margin);
    fplName.top = new FormAttachment(cpName, margin);
    plName.setLayoutData(fplName);
    pName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    pName.setToolTipText(BaseMessages.getString(PKG, "DynamoDBInput.Port.Tooltip"));
    props.setLook(pName);
    pName.addModifyListener(lsMod);
    fpName = new FormData();
    fpName.left = new FormAttachment(middle, 0);
    fpName.top = new FormAttachment(cpName, margin);
    fpName.right = new FormAttachment(100, 0);
    pName.setLayoutData(fpName);
    
    // Username
    dslName = new Label(shell, SWT.RIGHT);
    dslName.setText(BaseMessages.getString(PKG, "DynamoDBInput.Username.Label"));
    props.setLook(dslName);
    fdslName = new FormData();
    fdslName.left = new FormAttachment(0, 0);
    fdslName.right = new FormAttachment(middle, -margin);
    fdslName.top = new FormAttachment(pName, margin);
    dslName.setLayoutData(fdslName);
    dsName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    dsName.setToolTipText(BaseMessages.getString(PKG, "DynamoDBInput.Username.Tooltip"));
    props.setLook(dsName);
    dsName.addModifyListener(lsMod);
    fdsName = new FormData();
    fdsName.left = new FormAttachment(middle, 0);
    fdsName.top = new FormAttachment(pName, margin);
    fdsName.right = new FormAttachment(100, 0);
    dsName.setLayoutData(fdsName);
    
    // Password
    tlName = new Label(shell, SWT.RIGHT);
    tlName.setText(BaseMessages.getString(PKG, "DynamoDBInput.Password.Label"));
    props.setLook(tlName);
    ftlName = new FormData();
    ftlName.left = new FormAttachment(0, 0);
    ftlName.right = new FormAttachment(middle, -margin);
    ftlName.top = new FormAttachment(dsName, margin);
    tlName.setLayoutData(ftlName);
    tName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    tName.setToolTipText(BaseMessages.getString(PKG, "DynamoDBInput.Password.Tooltip"));
    props.setLook(tName);
    tName.addModifyListener(lsMod);
    ftName = new FormData();
    ftName.left = new FormAttachment(middle, 0);
    ftName.top = new FormAttachment(dsName, margin);
    ftName.right = new FormAttachment(100, 0);
    tName.setLayoutData(ftName);
    */
    // table field
    lblTable = new Label(shell, SWT.RIGHT);
    lblTable.setText(BaseMessages.getString(PKG, "DynamoDBInput.Table.Label"));
    props.setLook(lblTable);
    flblTable = new FormData();
    flblTable.left = new FormAttachment(0, 0);
    flblTable.right = new FormAttachment(middle, -margin);
    flblTable.top = new FormAttachment(wConnection, 2 * margin);
    lblTable.setLayoutData(flblTable);
    table = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    table.setToolTipText(BaseMessages.getString(PKG, "DynamoDBInput.Table.Tooltip"));
    props.setLook(table);
    table.addModifyListener(lsMod);
    fTable = new FormData();
    fTable.left = new FormAttachment(middle, 0);
    fTable.top = new FormAttachment(wConnection, 2 * margin);
    fTable.right = new FormAttachment(100, 0);
    table.setLayoutData(fTable);

    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel }, margin, table);

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    };

    wCancel.addListener(SWT.Selection, lsCancel);
    wOK.addListener(SWT.Selection, lsOK);

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    getData();
    //activeCopyFromPrevious();
    //activeUseKey();

    BaseStepDialog.setSize(shell);

    shell.open();
    props.setDialogSize(shell, "DynamoDBInputDialogSize");
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return stepname;

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText(Const.nullToEmpty(stepname));
    if (input.getDatabaseMeta() != null) {
      wConnection.setText(input.getDatabaseMeta().getName());
    } else if (transMeta.nrDatabases() == 1) {
      wConnection.setText(transMeta.getDatabase(0).getName());
    }
    table.setText(Const.nullToEmpty(input.getTable()));

    wName.selectAll();
    wName.setFocus();
  }

  /**
   * Handles clicking cancel
   */
  private void cancel() {
    stepname = null;
    input.setChanged(changed);
    dispose();
  }

  private int showDatabaseWarning(boolean includeCancel) {
    MessageBox mb = new MessageBox(shell, SWT.OK | (includeCancel ? SWT.CANCEL : SWT.NONE) | SWT.ICON_ERROR);
    mb.setMessage(BaseMessages.getString(PKG, "DynamoDBInputDialog.InvalidConnection.DialogMessage"));
    mb.setText(BaseMessages.getString(PKG, "DynamoDBInputDialog.InvalidConnection.DialogTitle"));
    return mb.open();
  }

  /**
   * Saves data to the meta class instance
   */
  private void ok() {
    if (null == wName.getText() || "".equals(wName.getText().trim())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.StepJobEntryNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.JobEntryNameMissing.Msg"));
      mb.open();
      return;
    }
    stepname = wName.getText();
    //input.setName( wName.getText() );

    if (transMeta.findDatabase(wConnection.getText()) == null) {
      int answer = showDatabaseWarning(true);
      if (answer == SWT.CANCEL) {
        return;
      }
    } else {
      input.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()));
    }
    //input.setHost(cpName.getText());
    //input.setPort(Integer.parseInt(pName.getText()));
    //input.setUsername(dsName.getText());
    //input.setPassword(tName.getText());
    String tableField = table.getText();
    if (null == tableField || "".equals(tableField.trim())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "DynamoDBInputDialog.TableMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "DynamoDBInputDialog.TableMissing.Msg"));
      mb.open();
      return;
    }
    input.setTable(tableField);

    dispose();
  }

}