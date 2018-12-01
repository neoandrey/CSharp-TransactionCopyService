using System.Configuration;
using System.Collections.Specialized;
using System;
using System.Collections;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.IO;
using System.Diagnostics;
using System.Data.SQLite;
using System.Data;
using System.Windows.Forms;
using System.Data.OleDb;
using System.Threading;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;

namespace TranCopyService{
  public class ConnectionProperty{
  
  
	string sourceServer = "";
	string sourceUser = "";
	string sourcePassword = "";
	string sourceDatabase = "";
	//string sourceTable = "";
	
	string destinationServer = "";
	string destinationUser = "";
	string destinationPassword = "";
	string destinationDatabase = "";
	string destinationTable = "";
    bool isInitiliazed =false;
	 
	 Form  prompt = new Form()
            {
                Width = 400,
                Height = 300,
                FormBorderStyle = FormBorderStyle.Fixed3D,
                Text = "Type Connection Details",
                StartPosition = FormStartPosition.CenterScreen
            };
	
	SQLiteConnection liteConnect;
  //SqlConnection officeConnection;
	
     Label sourceServerLabel = new Label() { Left = 10, Top = 15,Width =  120 , Height=20, Text = "Source Server: " };
	 TextBox sourceServerTextBox = new TextBox() { Left =140, Top = 15, Width = 200 , Height=20};			
	 Label sourceDatabaseLabel = new Label() { Left = 10, Top = 40 , Width = 100 , Height=20, Text = "Source Database: " };
	 TextBox sourceDatabaseTextBox = new TextBox() { Left = 140, Top = 40, Width = 200 , Height=20};
	 Label sourceUserLabel = new Label() { Left = 10, Top = 65 , Width = 100 , Height=20, Text = "Sourcer User: " };
	 TextBox sourceUserTextBox = new TextBox() { Left = 140, Top = 65, Width = 200 , Height=20};
	 Label sourcePasswordLabel = new Label() { Left = 10, Top = 90 , Width = 120  , Height=20, Text = "Source Password: " };
	 TextBox sourcePasswordText = new TextBox() { Left = 140, Top = 90, Width = 200 , Height=20,PasswordChar='*'};
	 Label destinationServerLabel = new Label() { Left = 10, Top = 120 , Width = 120  , Height=20, Text = "Destination Server: " };
	 TextBox destinationServerTextBox = new TextBox() { Left = 140, Top = 120, Width = 200 , Height=20};
	 Label destinationDatabaseLabel = new Label() { Left = 10, Top = 145 , Width = 120  , Height=20, Text = "Destination Database: " };
	 TextBox destinationDatabaseTextBox = new TextBox() { Left = 140, Top = 145, Width = 200 , Height=20};
	 Label   destinationUsernameLabel = new Label() { Left = 10, Top = 170 , Width =120  , Height=20, Text = "Destination Username: " };
	 TextBox destinationUserTextBox  = new TextBox() { Left = 140, Top = 170, Width = 200 , Height=20};
	 Label  destinationPasswordLabel= new Label() { Left = 10, Top = 195 , Width = 120  , Height=20, Text = "Destination Password: " };
	 TextBox destinationPasswordTextBox = new TextBox() { Left = 140, Top = 195, Width = 200 , Height=20,PasswordChar='*'}; 
	 Button cancelBttn = new Button() { Text = "Cancel", Left = 140, Width = 100, Top = 220};
	 Button okayBttn = new Button() { Text = "Ok", Left = 240, Width = 100, Top = 220 };
	
  public ConnectionProperty(){
	  
	  
	  
  }
   public string getSourceServer()
        {
            return this.sourceServer;
        }

        public string getSourceUser()
        {
            return this.sourceUser;
        }
        public string getSourcePassword()
        {
            return this.sourcePassword;
        }
        public string getSourceDatabase()
        {
            return this.sourceDatabase;
        }
        public string getDestinationServer()
        {
            return this.destinationServer;
        }

        public string getDestinationUser()
        {
            return this.destinationUser;
        }
        public string getDestinationPassword()
        {
            return this.destinationPassword;
        }
        public string getDestinationDatabase()
        {
            return this.destinationDatabase;
        }

        public void setSourceServer(string server)
        {
            this.sourceServer = server;
        }

        public void setSourceUser(string user)
        {
            this.sourceUser = user;
        }
        public void setSourcePassword(string password)
        {
            this.sourcePassword = password;
        }
        public void setSourceDatabase(string database)
        {
            this.sourceDatabase = database;
        }

        public void setDestinationServer(string server)
        {
            this.destinationServer = server;
        }

        public void setDestinationUser(string user)
        {
            this.destinationUser = user;
        }
        public void setDestinationPassword(string password)
        {
            this.destinationPassword = password;
        }
        public void setDestinationDatabase(string database)
        {
            this.destinationDatabase = database;
        }
        public void setDestinationTable(string table)
        {
            this.destinationTable = table;
        }

  
  public ConnectionProperty(string source, string sourceDatabase){
      try{
		if (!File.Exists(".\\db\\storekeeper.sqlite"))
		{
			SQLiteConnection.CreateFile(".\\db\\storekeeper.sqlite");
		}
		liteConnect = new SQLiteConnection("Data Source=.\\db\\storekeeper.sqlite;Version=3;");
		liteConnect.Open();	
		setSourceServer(source);
		setSourceDatabase(sourceDatabase);
		if(!string.IsNullOrEmpty(source) && !string.IsNullOrEmpty(sourceDatabase) ){

			 initConnectionProperty( source,  sourceDatabase);
		} else {
			Console.WriteLine("Source :" +source+ " or Source Database "+sourceDatabase+ " is empty.");
		}
       
	    if(!this.isInitiliazed){
			getUserConnectionDetails();
			if(this.sourceServer.Length!=0  && this.sourceDatabase.Length!=0  && this.sourceUser.Length!=0 && this.sourcePassword.Length!=0 &&  this.destinationServer.Length!=0 && this.destinationDatabase.Length!=0 && this.destinationUser.Length!=0 && this.destinationPassword.Length!= 0 ){
				
				//getConnectionProperty( this.getSourceServer(), this.getSourceDatabase(),  this.getDestinationServer(), this.getDestinationDatabase());
				prompt.Dispose();
			}
			
		} else{
			
			Console.WriteLine("Connection properties sucessfully loaded.");
		}
		  liteConnect.Close();
		}catch(Exception e){
			Console.WriteLine(e.Message);
		   Console.WriteLine(e.StackTrace);

			Environment.Exit(0);
		}
  }
  
    
  
  public void  saveConnectionDetails(){
	   try{
			string sql = "insert into server_connection_details ([source_address],[source_user_name],[source_password],[source_database_name]) values ('"
			+this.getSourceServer()+"','"
			+new ConnectionCipher().scramble(this.getSourceUser())+"','"
			+new ConnectionCipher().scramble(this.getSourcePassword())+"','"
			+this.getSourceDatabase()+
			"')";
			 Console.WriteLine("Running: "+sql);
			SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
			command.CommandTimeout = -1;
			command.ExecuteNonQuery();
			command.Dispose();
			MessageBox.Show("Connection detials have been successfully saved", "Add Connection Details",MessageBoxButtons.OK,MessageBoxIcon.Information);
			 } catch(Exception e){
		  Console.WriteLine(e.Message);
		  Console.WriteLine(e.StackTrace);
	  }

  }
  public bool checkUserTableExist(){ 
   try{
	  string sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='server_connection_details';";
	  Console.WriteLine("Running: "+sql);
	 SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
	  Object result = command.ExecuteScalar();
	  command.Dispose();
	  Console.WriteLine("result: "+result.ToString());
	  if(result.ToString() == "1"){
		  
		  
		   return true;
	  }
	  } catch(Exception e){
		  Console.WriteLine(e.Message);
		  Console.WriteLine(e.StackTrace);
	  }
	   return false;
  }
  public  bool createConnectionTable(){
		try{
				string sql = "CREATE TABLE server_connection_details ( source_address TEXT, source_user_name TEXT, source_password TEXT,  source_database_name  TEXT)";
				Console.WriteLine("Running: "+sql);
				SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
				if(command.ExecuteNonQuery()>=0) return true;
		        command.Dispose();
		  } catch(Exception e){
		  Console.WriteLine(e.Message);
		  Console.WriteLine(e.StackTrace);
	  }
		 return false;
		
  }
  
  public  bool getConnectionProperty(string source,  string source_database){
	   bool isInit  = false;
	 try{
				string sql = "SELECT   [source_address]      ,[source_user_name]      ,[source_password]      ,[source_database_name]       FROM  server_connection_details WHERE   source_address = '"+source+"' AND  source_database_name = '"+source_database+"'";
				Console.WriteLine("Running: "+sql);
				//liteConnect.Open();
				SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
				command.CommandTimeout = -1;
				command.ExecuteNonQuery();
				SQLiteDataReader reader = command.ExecuteReader();				
	       if (reader != null) {
			while (reader.Read()){	
                 Console.WriteLine("initializing");
				  this.setSourceServer((string)reader["source_address"]);
				  this.setSourceDatabase((string)reader["source_database_name"]);
				  this.setSourceUser( new ConnectionCipher().decipher((string)reader["source_user_name"]));
				  this.setSourcePassword( new ConnectionCipher().decipher((string)reader["source_password"]));	 
				  isInit  =true;
			  }
	  }
	  else{	  
		  Console.WriteLine("Reader returned empty");
	  }
				command.Dispose();
	 } catch(Exception e){
		  Console.WriteLine(e.Message);
		  Console.WriteLine(e.StackTrace);
	  }
       return isInit;
	  
  }
  
  public void getUserConnectionDetails(){
	

		 sourceServerTextBox.Text =this.getSourceServer();
		 destinationServerTextBox.Text = this.getDestinationServer();
		 sourceDatabaseTextBox.Text = this.getSourceDatabase();
		 destinationDatabaseTextBox.Text = this.getDestinationDatabase();
		    prompt.Controls.Add(sourceServerLabel);
            prompt.Controls.Add(sourceServerTextBox);
		    prompt.Controls.Add(sourceDatabaseLabel);
            prompt.Controls.Add(sourceDatabaseTextBox);
			prompt.Controls.Add(sourceUserLabel);
            prompt.Controls.Add(sourceUserTextBox);
            prompt.Controls.Add(sourcePasswordLabel);
		    prompt.Controls.Add(sourcePasswordText);
			prompt.Controls.Add(cancelBttn);
			prompt.Controls.Add(okayBttn);
			
			 cancelBttn.Click+= (sender, e) => { 
			   prompt.DialogResult = DialogResult.None; 
				DialogResult dr = MessageBox.Show("Do you want to exit?", "storekeeper", MessageBoxButtons.YesNo, MessageBoxIcon.Question);
				if(dr == DialogResult.Yes)
				{
					Environment.Exit(0);
				}
			 };
			
		   okayBttn.Click+= (sender, e) => { 
		   if(sourceServerTextBox.Text.Trim().Length ==0){
			  MessageBox.Show("Source address cannot be empty", "storekeeper",MessageBoxButtons.OK, MessageBoxIcon.Warning);
			  
		   }else if(sourceDatabaseTextBox.Text.Trim().Length ==0 ){
			   
			     MessageBox.Show("Source database cannot be empty", "storekeeper",MessageBoxButtons.OK, MessageBoxIcon.Warning);
			   
		   }else if(sourceUserTextBox.Text.Trim().Length ==0 ){
			     MessageBox.Show("Source user cannot be empty", "storekeeper",MessageBoxButtons.OK, MessageBoxIcon.Warning);
			   
			   
		   } else{
		    setSourceServer(sourceServerTextBox.Text);
		    setSourceDatabase(sourceDatabaseTextBox.Text);
		    setSourceUser(sourceUserTextBox.Text);
		    setSourcePassword(sourcePasswordText.Text);	  
		    saveConnectionDetails();
			prompt.DialogResult = DialogResult.OK;
		   }
		  
		 
	   };
	    
	   prompt.ShowDialog();
  }
  public void initConnectionProperty(string source,  string source_database){
	  try{
	  if(checkUserTableExist()){
		  this.isInitiliazed =  getConnectionProperty(source,source_database );
		
	  }else{
		    createConnectionTable();
 
	  }
	  
	  }catch(Exception e){
		  
		  Console.WriteLine(e.StackTrace);
	  }
	  
  }
  
  public static void Main(string[] args){
	  new ConnectionProperty(args[0],args[1]);
	  
	  
  }
  
  }

}