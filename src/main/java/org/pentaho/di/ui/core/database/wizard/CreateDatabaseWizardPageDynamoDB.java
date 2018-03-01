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

package org.pentaho.di.ui.core.database.wizard;

import org.pentaho.di.ui.core.widget.LabelComboVar;

import org.eclipse.jface.wizard.IWizard;
import org.eclipse.jface.wizard.IWizardPage;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.aws.dynamodb.DynamoDBDatabaseMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;

/**
 *
 * On page one we select the database connection DynamoDB specific settings 
 *
 * @author Adam Fowler <adam.fowler@hitachivantara.com>
 * @since 28-02-2018
 */
public class CreateDatabaseWizardPageDynamoDB extends WizardPage {
  private static Class<?> PKG = DynamoDBDatabaseMeta.class; // for i18n purposes, needed by Translator2!!

  // DynamoDB Server
  private Label wlHost, wlPort, wlName;
  private Text wHost, wPort, wName;
  private LabelComboVar cAuthScheme, cDeploymentType;
  private FormData fdlHost, fdlPort, fdlName, fdlRegion;
  private FormData fdHost, fdPort, fdName, fdAuthScheme, fdDeploymentType, fdRegion;

  private PropsUI props;
  private DatabaseMeta info;

  public CreateDatabaseWizardPageDynamoDB(String arg, PropsUI props, DatabaseMeta info) {
    super(arg);
    this.props = props;
    this.info = info;

    setTitle(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.DialogTitle"));
    setDescription(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.DialogMessage"));

    setPageComplete(false);
  }

  public void createControl(Composite parent) {
    int margin = Const.MARGIN;
    int middle = props.getMiddlePct();

    // create the composite to hold the widgets
    Composite composite = new Composite(parent, SWT.NONE);
    props.setLook(composite);

    FormLayout compLayout = new FormLayout();
    compLayout.marginHeight = Const.FORM_MARGIN;
    compLayout.marginWidth = Const.FORM_MARGIN;
    composite.setLayout(compLayout);

    // HOST
    wlHost = new Label(composite, SWT.RIGHT);
    wlHost.setText(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.Host.Label"));
    props.setLook(wlHost);
    fdlHost = new FormData();
    fdlHost.top = new FormAttachment(0, 0);
    fdlHost.left = new FormAttachment(0, 0);
    fdlHost.right = new FormAttachment(middle, 0);
    wlHost.setLayoutData(fdlHost);
    wHost = new Text(composite, SWT.SINGLE | SWT.BORDER);
    props.setLook(wHost);
    fdHost = new FormData();
    fdHost.top = new FormAttachment(0, 0);
    fdHost.left = new FormAttachment(middle, margin);
    fdHost.right = new FormAttachment(100, 0);
    wHost.setLayoutData(fdHost);
    wHost.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent arg0) {
        setPageComplete(false);
      }
    });

    wlPort = new Label(composite, SWT.RIGHT);
    wlPort.setText(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.Port.Label"));
    props.setLook(wlPort);
    fdlPort = new FormData();
    fdlPort.top = new FormAttachment(wHost, margin);
    fdlPort.left = new FormAttachment(0, 0);
    fdlPort.right = new FormAttachment(middle, 0);
    wlPort.setLayoutData(fdlPort);
    wPort = new Text(composite, SWT.SINGLE | SWT.BORDER);
    props.setLook(wPort);
    fdPort = new FormData();
    fdPort.top = new FormAttachment(wHost, margin);
    fdPort.left = new FormAttachment(middle, margin);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);
    wPort.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent arg0) {
        setPageComplete(false);
      }
    });
    
    // Database name
    wlName = new Label(composite, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.DatabaseName.Label"));
    props.setLook(wlName);
    fdlName = new FormData();
    fdlName.top = new FormAttachment(wPort, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    wlName.setLayoutData(fdlName);
    wName = new Text(composite, SWT.SINGLE | SWT.BORDER);
    props.setLook(wName);
    fdName = new FormData();
    fdName.top = new FormAttachment(wPort, margin);
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    wName.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent arg0) {
        setPageComplete(false);
      }
    });

    // TODO replace with combo box
    /*
    wlAuthScheme = new Label(composite, SWT.RIGHT);
    wlAuthScheme.setText(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.AuthScheme.Label"));
    props.setLook(wlAuthScheme);
    fdlAuthScheme = new FormData();
    fdlAuthScheme.top = new FormAttachment(wName, margin);
    fdlAuthScheme.left = new FormAttachment(0, 0);
    fdlAuthScheme.right = new FormAttachment(middle, 0);
    wlAuthScheme.setLayoutData(fdlAuthScheme);
    */
    cAuthScheme = new LabelComboVar(info, composite, BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.AuthScheme.Label"),
        BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.AuthScheme.Tooltip"));
    cAuthScheme.setItems(new String[] {"credentials","credentialsfile"});
    cAuthScheme.getComboWidget().setEditable(false);
    //wAuthScheme = new Text(composite, SWT.SINGLE | SWT.BORDER);
    props.setLook(cAuthScheme);
    fdAuthScheme = new FormData();
    fdAuthScheme.top = new FormAttachment(wName, margin);
    fdAuthScheme.left = new FormAttachment(0, 0);
    fdAuthScheme.right = new FormAttachment(100, 0);
    cAuthScheme.setLayoutData(fdAuthScheme);
    cAuthScheme.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent arg0) {
        setPageComplete(false);
      }
    });

    
    cDeploymentType = new LabelComboVar(info, composite, BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.AuthScheme.Label"),
        BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.AuthScheme.Tooltip"));
    cDeploymentType.setItems(new String[] {"local","aws"});
    cDeploymentType.getComboWidget().setEditable(false);
    props.setLook(cDeploymentType);
    fdDeploymentType = new FormData();
    fdDeploymentType.top = new FormAttachment(cAuthScheme, margin);
    fdDeploymentType.left = new FormAttachment(0, 0);
    fdDeploymentType.right = new FormAttachment(100, 0);
    cDeploymentType.setLayoutData(fdDeploymentType);
    cDeploymentType.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent arg0) {
        setPageComplete(false);
      }
    });

    // TODO region controls here


/*
    wlUsername = new Label(composite, SWT.RIGHT);
    wlUsername.setText(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.Username.Label"));
    props.setLook(wlUsername);
    fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(wAuthScheme, margin);
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, 0);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new Text(composite, SWT.SINGLE | SWT.BORDER);
    props.setLook(wUsername);
    fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wAuthScheme, margin);
    fdUsername.left = new FormAttachment(middle, margin);
    fdUsername.right = new FormAttachment(100, 0);
    wUsername.setLayoutData(fdUsername);
    wUsername.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent arg0) {
        setPageComplete(false);
      }
    });

    wlPassword = new Label(composite, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.Password.Label"));
    props.setLook(wlPassword);
    fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(wUsername, margin);
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, 0);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new Text(composite, SWT.SINGLE | SWT.BORDER);
    props.setLook(wPassword);
    fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wUsername, margin);
    fdPassword.left = new FormAttachment(middle, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);
    wPassword.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent arg0) {
        setPageComplete(false);
      }
    });
    */

    // set the composite as the control for this page
    setControl(composite);
  }

  public void setData() {
    wHost.setText(Const.NVL(info.getHostname(),"localhost"));
    wPort.setText(Const.NVL(info.getDatabasePortNumberString(), "8000"));
    wName.setText(Const.NVL(info.getDatabaseName(), "Documents"));
    cAuthScheme.setText(Const.NVL(info.getAttributes().getProperty(DynamoDBDatabaseMeta.ATTRIBUTE_AUTHSCHEME, ""), "credentials"));
    cDeploymentType.setText(Const.NVL(info.getAttributes().getProperty(DynamoDBDatabaseMeta.ATTRIBUTE_DEPLOYMENT, ""), "local"));
    //wUsername.setText(Const.NVL(info.getAttributes().getProperty(DynamoDBDatabaseMeta.ATTRIBUTE_USERNAME, ""), ""));
    //wPassword.setText(Const.NVL(info.getAttributes().getProperty(DynamoDBDatabaseMeta.ATTRIBUTE_PASSWORD, ""), ""));
  }

  public boolean canFlipToNextPage() {
    String server = wHost.getText() != null ? wHost.getText().length() > 0 ? wHost.getText() : null : null;
    String port = wPort.getText() != null
        ? wPort.getText().length() > 0 ? wPort.getText() : null
        : null;
    String authScheme = cAuthScheme.getText() != null
        ? cAuthScheme.getText().length() > 0 ? cAuthScheme.getText() : null
        : null;
    //String username = wUsername.getText() != null ? wUsername.getText().length() > 0 ? wUsername.getText() : null
    //    : null;
    //String password = wPassword.getText() != null ? wPassword.getText().length() > 0 ? wPassword.getText() : null
    //    : null;

    if (authScheme == null) {
      setErrorMessage(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.ErrorMessage.InvalidInput"));
      return false;
    } else {
      getDatabaseInfo();
      setErrorMessage(null);
      setMessage(BaseMessages.getString(PKG, "CreateDatabaseWizardPageDynamoDB.Message.Next"));
      return true;
    }

  }

  public DatabaseMeta getDatabaseInfo() {
    if (wHost.getText() != null && wHost.getText().length() > 0) {
    //  info.getAttributes().put(DynamoDBDatabaseMeta.ATTRIBUTE_HOST,wHost.getText());
      info.setHostname(wHost.getText());
    }

    if (wPort.getText() != null && wPort.getText().length() > 0) {
      //  info.getAttributes().put(DynamoDBDatabaseMeta.ATTRIBUTE_PORT, wPort.getText());
      info.setDBPort(wPort.getText());
    }

    if (wName.getText() != null && wName.getText().length() > 0) {
      info.setDBName(wName.getText());
    }

    if ( cAuthScheme.getText() != null && cAuthScheme.getText().length() > 0 ) {
      info.getAttributes().put( DynamoDBDatabaseMeta.ATTRIBUTE_AUTHSCHEME, cAuthScheme.getText());
    }

    //if ( wUsername.getText() != null && wUsername.getText().length() > 0 ) {
    //  info.getAttributes().put( DynamoDBDatabaseMeta.ATTRIBUTE_USERNAME, wUsername.getText());
    //}

    //if ( wPassword.getText() != null && wPassword.getText().length() > 0 ) {
    //  info.getAttributes().put( DynamoDBDatabaseMeta.ATTRIBUTE_PASSWORD, wPassword.getText());
    //}
    return info;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.eclipse.jface.wizard.WizardPage#getNextPage()
   */
  public IWizardPage getNextPage() {
    IWizard wiz = getWizard();
    return wiz.getPage("2");
  }

}
