using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Data;
using System.Globalization;
using System.Net.Mail;
using System.Data.OleDb;
using System.Data.DataSetExtensions;
using System.Data.SQLite;
using System.Reflection;

namespace ArbiterCopyService{
            public class  ArbiterCopy {

                 internal  static System.Data.DataTable[]  	       arbiterCopyTables;
				 internal  static Thread[]						   arbiterCopyThreads;


                 internal  static System.Data.DataTable[]  	       arbiterCopyFilterTables;
				 internal  static Thread[]						   arbiterCopyFilterThreads;            
		         internal  static Dictionary<string, string>       connectionStringMap     			    = new Dictionary<string,string>();	
			     internal  static Dictionary<string, string>       partitionSizeIntervalMap   			= new Dictionary<string,string>();	    
				 internal  static string                           sourceServerConnectionString;   
				 internal  static string                           destServerConnectionString;

				  internal  static string                          stagingConnectionString;
                 internal  static int 							   datePartitionInterval			    = 0;
				 internal  static Dictionary<int,Thread>           arbiterCopyThreadMap                 = new Dictionary<int,Thread>();

				  internal  static Dictionary<int,Thread>          arbiterCopyFilterThreadMap           = new Dictionary<int,Thread>();
				 internal  static HashSet<Thread>          		   runningThreadSet                     = new HashSet<Thread>();
				 internal  static Dictionary<Thread, DataTable>    threadTableMap     			        = new Dictionary<Thread, DataTable>();
				 internal  static int                              numberOfTables						= 0; 
				 internal  static bool							   isFirstInsert                        = true;   
                 internal  static StringBuilder					   emailError                           =  new StringBuilder();

				 internal  static  Dictionary<int,int>             indexSizeMap							= new Dictionary<int,int> ();

				 internal  static  DataTable					    schemaTable                         =  new DataTable();

				 internal  static  DataTable					    filterTableSchema                   =  new DataTable();

				 internal    const int					        	DAILY_COPY_MODE						= 0;

				 internal    const int                              SPECIFIC_COPY_MODE                  =  1;

                internal     const  int                            RANGE_COPY_MODE						= 2;   

				internal     static readonly object                locker                               =            new object();

				internal      static  Dictionary <string,DataTable>  reportTableMap  				    = 	 new Dictionary <string,DataTable> ();
				internal    static StringBuilder emailBody											    = new StringBuilder();			
				internal  static  Dictionary<int, string>  copyDateMap                     				= new  Dictionary<int, string> ();
				internal  static  Dictionary<int, string>  copyTableNameMap                				= new  Dictionary<int, string> ();
				internal  static  Dictionary<int, string>  copyFilterTableNameMap          				= new  Dictionary<int, string> ();
				internal  static  Dictionary<int, string>  copyStartTimeMap	     		   				= new  Dictionary<int, string> ();
				internal  static  Dictionary<int, string>  copyFilterStartTimeMap          				= new  Dictionary<int, string> ();
				internal  static  Dictionary<int, string>  copyEndTimeMap          		   				= new Dictionary<int, string> ();
				internal  static  Dictionary<int, string>  copyFilterEndTimeMap            				= new  Dictionary<int, string> ();
				internal  static  Dictionary<int, long>    copyTransCountMap       		   				= new Dictionary<int, long> 	 ();
				internal  static  Dictionary<int, long>    copyFilterTransCountMap  	  				= new Dictionary<int, long>   ();
				internal  static  Dictionary<int, long>    copyInsertedTransCountMap       				= new Dictionary<int, long>   ();
				internal  static  Dictionary<int, long>    copyFilterInsertedTransCountMap			    = new Dictionary<int, long>   ();

				internal  static  string[]                  copyFilterScript;

                   


				 public ArbiterCopy(){
						new  ArbiterCopyUtilLibrary();
				DataTable table = new DataTable("PropertiesTable");
			
				DataColumn column;
				DataRow row;  
				column				        = new DataColumn();
				
				column.DataType 		     = System.Type.GetType("System.Int32");
				column.ColumnName 		     = "no.";
				column.ReadOnly 		     = true;
				column.Unique 				 = true;
				column.AutoIncrement 		 = true;
				table.Columns.Add(column);
				column				     	 = new DataColumn();
				column.DataType 			 = System.Type.GetType("System.String");
				column.ColumnName 		     = "parameter_name";
				column.AutoIncrement 		 = false;
				column.Caption 				 = "ParameterName";
				column.ReadOnly 			 = false;
				column.Unique 				 = true;
				table.Columns.Add(column);
				column 						 = new DataColumn();
				column.DataType 			 = System.Type.GetType("System.String");
				column.ColumnName 			 = "parameter_value";
				column.AutoIncrement 		 = false;
				column.Caption 				 = "ParameterValue";
				column.ReadOnly 			 = false;
				column.Unique 				 = false;
				table.Columns.Add(column);

				DataColumn[] PrimaryKeyColumns = new DataColumn[1];
				PrimaryKeyColumns[0] = table.Columns["no."];
				table.PrimaryKey = PrimaryKeyColumns;
				
				foreach (PropertyInfo prop in ArbiterCopyUtilLibrary.arbiterConfig.GetType().GetProperties())
				{
					
					if(null!=prop.GetValue(ArbiterCopyUtilLibrary.arbiterConfig) && null!= prop.Name){
						row = table.NewRow();
						row["parameter_name"]  = prop.Name;
						row["parameter_value"] = string.IsNullOrWhiteSpace(prop.GetValue(ArbiterCopyUtilLibrary.arbiterConfig,  null).ToString())?"NULL":prop.GetValue(ArbiterCopyUtilLibrary.arbiterConfig,  null).ToString();
						ArbiterCopyUtilLibrary.writeToLog(prop.Name+": "+row["parameter_value"]);
						table.Rows.Add(row);
					}
						
				}
	
				reportTableMap.Add("Arbiter Copy Session Parameters",table);


						startArbiterCopy();
            	
				 }

                public ArbiterCopy(string  config){
				  
				
					string  nuConfig   = config.Contains("\\\\")? config:config.Replace("\\", "\\\\");
					if(File.Exists(nuConfig)){
					new  ArbiterCopyUtilLibrary(nuConfig);
					DataTable table = new DataTable("PropertiesTable");
			
				DataColumn column;
				DataRow row;  
				column				        = new DataColumn();
				
				column.DataType 		     = System.Type.GetType("System.Int32");
				column.ColumnName 		     = "no.";
				column.ReadOnly 		     = true;
				column.Unique 				 = true;
				column.AutoIncrement 		 = true;
				table.Columns.Add(column);
				column				     	 = new DataColumn();
				column.DataType 			 = System.Type.GetType("System.String");
				column.ColumnName 		     = "parameter_name";
				column.AutoIncrement 		 = false;
				column.Caption 				 = "ParameterName";
				column.ReadOnly 			 = false;
				column.Unique 				 = true;
				table.Columns.Add(column);
				column 						 = new DataColumn();
				column.DataType 			 = System.Type.GetType("System.String");
				column.ColumnName 			 = "parameter_value";
				column.AutoIncrement 		 = false;
				column.Caption 				 = "ParameterValue";
				column.ReadOnly 			 = false;
				column.Unique 				 = false;
				table.Columns.Add(column);

				DataColumn[] PrimaryKeyColumns = new DataColumn[1];
				PrimaryKeyColumns[0] = table.Columns["no."];
				table.PrimaryKey = PrimaryKeyColumns;
				
				foreach (PropertyInfo prop in ArbiterCopyUtilLibrary.arbiterConfig.GetType().GetProperties())
				{
					
					if(null!=prop.GetValue(ArbiterCopyUtilLibrary.arbiterConfig) && null!= prop.Name){
						row = table.NewRow();
						row["parameter_name"]  = prop.Name;
						row["parameter_value"] = string.IsNullOrWhiteSpace(prop.GetValue(ArbiterCopyUtilLibrary.arbiterConfig,  null).ToString())?"NULL":prop.GetValue(ArbiterCopyUtilLibrary.arbiterConfig,  null).ToString();
						ArbiterCopyUtilLibrary.writeToLog(prop.Name+": "+row["parameter_value"]);
						table.Rows.Add(row);
					}
						
				}
	
				reportTableMap.Add("Arbiter Copy Session Parameters",table);
                   	startArbiterCopy();

					} else{
						
						Console.WriteLine("The specified configuration file: "+nuConfig+" does not exist. Please review configuration file parameter( -c ).");
									
					}
					
			  }

			   public bool hasQuotes(String rawString){

				   	return rawString.StartsWith("\'") &&rawString.EndsWith("\'");


			   }
			
			  public string quoteString(String rawString){

						  return  "\'"+rawString+"\'";

			  }
                public void startArbiterCopy(){
                   try{ 
                        
                        initConnectionStrings();
						if(!File.Exists(ArbiterCopyUtilLibrary.copyScript)){

							 Console.WriteLine("Report source script "+ArbiterCopyUtilLibrary.copyScript+" does not exist.");
							 ArbiterCopyUtilLibrary.writeToLog("Report source script "+ArbiterCopyUtilLibrary.copyScript+" does not exist.");
							 Environment.Exit(0);

						}
						if( serverIsReachable(connectionStringMap["source"])){
							if( string.IsNullOrEmpty(ArbiterCopyUtilLibrary.destinationTable)){
								Console.WriteLine("No table has been specified for this Arbiter Copy session");
								ArbiterCopyUtilLibrary.writeToLog("No table has been specified for this Arbiter Copy session");
                                Environment.Exit(0);
							}

							DateTime startParameterValue	   =  DateTime.Now;// DateTime.ParseExact(ArbiterCopyUtilLibrary.copyStartParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                            DateTime endParameterValue	       =  DateTime.Now;// DateTime.ParseExact(ArbiterCopyUtilLibrary.copyEndParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture); ;
                           
						     switch(ArbiterCopyUtilLibrary.arbiterCopyMode){

								 case DAILY_COPY_MODE:
								 startParameterValue = string.IsNullOrEmpty(ArbiterCopyUtilLibrary.copyStartParameterValue) || ArbiterCopyUtilLibrary.copyStartParameterValue =="default" ?DateTime.Now:DateTime.ParseExact(ArbiterCopyUtilLibrary.copyStartParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);

								int  numOfPrevDays          = ArbiterCopyUtilLibrary.numOfDaysFromStart;
								endParameterValue           = startParameterValue.AddDays(-1*numOfPrevDays);

                            
							long startParamVal = long.Parse(startParameterValue.ToString("yyyyMMdd"));
							
							long endParamVal   = long.Parse(endParameterValue.ToString("yyyyMMdd"));

							long currentVal    = startParamVal;
                             numberOfTables = 0;
								while(currentVal   >= endParamVal){
									Console.WriteLine("currentVal: "+currentVal.ToString());
									copyDateMap.Add(numberOfTables, currentVal.ToString());
									ArbiterCopyUtilLibrary.writeToLog("currentVal: "+currentVal.ToString());
								    partitionSizeIntervalMap.Add(currentVal.ToString(), currentVal.ToString());
									currentVal -=1; 
									++numberOfTables;								
								}
								 break;
								 case SPECIFIC_COPY_MODE:
								  startParameterValue	   =  DateTime.ParseExact(ArbiterCopyUtilLibrary.copyStartParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                 endParameterValue	       = DateTime.ParseExact(ArbiterCopyUtilLibrary.copyEndParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture); ;
                                  numberOfTables           =  0;
                                    
								   foreach(string  dateStr in  ArbiterCopyUtilLibrary.arbiterCopySpecificParamterValues){
									    copyDateMap.Add(numberOfTables,DateTime.ParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture).ToString("yyyy-MM-dd"));
										partitionSizeIntervalMap.Add(DateTime.ParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture).ToString("yyyy-MM-dd"), DateTime.ParseExact(dateStr, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture).ToString("yyyy-MM-dd"));
                                        ++numberOfTables;
								   }

								 break;
								 case  RANGE_COPY_MODE:
								  startParameterValue	   =  DateTime.ParseExact(ArbiterCopyUtilLibrary.copyStartParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                   endParameterValue	       = DateTime.ParseExact(ArbiterCopyUtilLibrary.copyEndParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture); ;
                                   numberOfTables   = 0;

									    startParamVal = long.Parse(startParameterValue.ToString( "yyyyMMdd"));
										currentVal    = startParamVal;
									    endParamVal   = long.Parse(endParameterValue.ToString( "yyyyMMdd"));
										while( currentVal <= endParamVal){
                                                copyDateMap.Add(numberOfTables, currentVal.ToString());
												partitionSizeIntervalMap.Add(currentVal.ToString(), currentVal.ToString());
												++numberOfTables;
												currentVal   = currentVal  +1;
										}
								 break;
							 }
									
                  			    string copyScriptStr      =   File.ReadAllText(ArbiterCopyUtilLibrary.copyScript);
							    string filterScriptStr    =  File.ReadAllText( ArbiterCopyUtilLibrary.arbiterFilterCopyScript);

                    			getSchemaTable( copyScriptStr.Replace(ArbiterCopyUtilLibrary.copyStartParameter,"\'2000-01-01\'"));
								getFilterTableSchema( filterScriptStr.Replace(ArbiterCopyUtilLibrary.copyStartParameter,"2000-01-01")
								                                     .Replace("ARBITER_DESTINATION_TABLE",ArbiterCopyUtilLibrary.destinationTable));

								if(schemaTable.Columns.Count > 0 ){

								arbiterCopyTables           		     = new System.Data.DataTable[numberOfTables];
								arbiterCopyThreads     		 			 = new Thread[numberOfTables];
								
							    arbiterCopyFilterTables      		     = new System.Data.DataTable[numberOfTables];
								arbiterCopyFilterThreads     			 = new Thread[numberOfTables];
								copyFilterScript						 = new string[numberOfTables];
								ArbiterCopyUtilLibrary.concurrentThreads = numberOfTables;
								

                                int threadCounter = 0;
								foreach(KeyValuePair<string, string>  param in partitionSizeIntervalMap){
									
								 string  copySQL	         	= copyScriptStr.Replace(ArbiterCopyUtilLibrary.copyStartParameter,param.Key);
								 copySQL    	      	        = copySQL.Replace(ArbiterCopyUtilLibrary.copyEndParameter, param.Value);
                                
								 string  copyFilterSQL	        = filterScriptStr.Replace(ArbiterCopyUtilLibrary.copyStartParameter,param.Key);
								 copyFilterSQL    	      	    = copyFilterSQL.Replace(ArbiterCopyUtilLibrary.copyEndParameter, param.Value)
								                                            .Replace("ARBITER_DESTINATION_TABLE",ArbiterCopyUtilLibrary.destinationTable);

								 
								
								  int  currentIndex				= threadCounter;
								
									  arbiterCopyThreads[currentIndex]  = 	new Thread(()=> {
										
										 runArbiterTransactionCopy("source",copySQL,currentIndex);
									
									});
									arbiterCopyThreads[currentIndex].Name    =   "arbiterCopyThread."+currentIndex.ToString();
									arbiterCopyThreadMap.Add(currentIndex,arbiterCopyThreads[currentIndex]);
									copyFilterScript[currentIndex]  		=	 copyFilterSQL;


									 arbiterCopyFilterThreads[currentIndex]  = 	new Thread(()=> {
										
										 runArbiterTransactionCopyFilter("destination",copyFilterSQL,currentIndex);
									
									});
									arbiterCopyFilterThreads[currentIndex].Name    =   "arbiterCopyThread."+currentIndex.ToString();
									arbiterCopyFilterThreadMap.Add(currentIndex,arbiterCopyFilterThreads[currentIndex]);
									++threadCounter;
                        			}
								
						        Console.WriteLine("Count: "+numberOfTables.ToString());
							    Console.WriteLine("Threads successfully initialized");
							    ArbiterCopyUtilLibrary.writeToLog("Threads successfully initialized");
							    Console.WriteLine("WAIT_INTERVAL: "+ArbiterCopyUtilLibrary.WAIT_INTERVAL.ToString());
							    ArbiterCopyUtilLibrary.writeToLog("WAIT_INTERVAL: "+ArbiterCopyUtilLibrary.WAIT_INTERVAL.ToString());
						        Console.WriteLine("ArbiterCopyUtilLibrary.concurrentThreads: "+ArbiterCopyUtilLibrary.concurrentThreads.ToString());
							
							//    foreach(Thread threadDetail in arbiterCopyThreads){
								for(int i=0; i< arbiterCopyThreads.Length; i++){


                                        Console.WriteLine("Starting thread: "+i.ToString());
										ArbiterCopyUtilLibrary.writeToLog("Starting thread: "+i.ToString());
										if(arbiterCopyThreads[i]!= null) {
									      arbiterCopyThreads[i].Start();
										  runningThreadSet.Add(arbiterCopyThreads[i]);
									   }
									    if(arbiterCopyFilterThreads[i]!= null) {
									      arbiterCopyFilterThreads[i].Start();
										  runningThreadSet.Add(arbiterCopyFilterThreads[i]);
									   }
									
									 
							}
							ArbiterCopyUtilLibrary.writeToLog("All threads started. Waiting for matching threads to finish");
							Console.WriteLine("All threads started. Waiting for matching threads to finish");
							 wait();
																				
							DataTable table = new DataTable("SessionTable");
							
								DataColumn column;
								DataRow row;  
								column				         = new DataColumn();
								
								column.DataType 		     = System.Type.GetType("System.Int32");
								column.ColumnName 		     = "no.";
								column.ReadOnly 		     = true;
								column.Unique 				 = true;
								column.AutoIncrement 		 = true;
								table.Columns.Add(column);
								column				     	 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 		     = "transation_date";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "TransactionDate";
								column.ReadOnly 			 = false;
								column.Unique 				 = true;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "copy_staging_table_name";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "CopyStagingTableName";
								column.ReadOnly 			 = false;
								column.Unique 				 = true;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "filter_staging_table_name";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "FilterStagingTableName";
								column.ReadOnly 			 = false;
								column.Unique 				 = true;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "transaction_copy_start_time";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "TransactionCopyStartTime";
								column.ReadOnly 			 = false;
								column.Unique 				 = false;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "filter_copy_start_time";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "FilterCopyStartTime";
								column.ReadOnly 			 = false;
								column.Unique 				 = false;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "transaction_copy_duration";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "TransactionCopyDuration";
								column.ReadOnly 			 = false;
								column.Unique 				 = false;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "filter_copy_duration";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "filterCopyDuration";
								column.ReadOnly 			 = false;
								column.Unique 				 = false;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "transaction_staging_count";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "TransactionStagingCount";
								column.ReadOnly 			 = false;
								column.Unique 				 = false;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "filter_staging_count";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "FilterStagingCount";
								column.ReadOnly 			 = false;
								column.Unique 				 = false;
								table.Columns.Add(column);
								column 						 = new DataColumn();
								column.DataType 			 = System.Type.GetType("System.String");
								column.ColumnName 			 = "inserted_transaction_count";
								column.AutoIncrement 		 = false;
								column.Caption 				 = "InsertedTransactionCount";
								column.ReadOnly 			 = false;
								column.Unique 				 = false;
								table.Columns.Add(column);


								DataColumn[] PrimaryKeyColumns = new DataColumn[1];
								PrimaryKeyColumns[0] = table.Columns["no."];
								table.PrimaryKey = PrimaryKeyColumns;

								string  filterTableName               = "";
								string  tranStagingTable              = "";
								for (int  i=0; i< numberOfTables;  i++){
								tranStagingTable                      = ArbiterCopyUtilLibrary.arbiterCopyTableNamePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType.ToLower()+"_"+i.ToString();
								filterTableName                       = ArbiterCopyUtilLibrary.arbiterCopyFilterTablePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType.ToLower()+"_"+i.ToString();

								copyTransCountMap.Add(i, getAggregateFromTable("COUNT",tranStagingTable, "*", "staging" ));
								copyFilterTransCountMap.Add(i, getAggregateFromTable("COUNT",filterTableName, "*", "staging" ));

								}

								for (int i=0; i<numberOfTables; i++){
																
																		
									row      				                      = table.NewRow();
									row["transation_date"]            			  = copyDateMap[i];
									row["copy_staging_table_name"]    			  = copyTableNameMap[i];
									row["filter_staging_table_name"]    		  = copyFilterTableNameMap[i];
									row["transaction_copy_start_time"]   		  = copyStartTimeMap[i];
									row["filter_copy_start_time"] 				  = copyFilterStartTimeMap[i] ;
									TimeSpan tranCopyDuration                     = DateTime.Parse(copyEndTimeMap[i], System.Globalization.CultureInfo.InvariantCulture).Subtract(DateTime.Parse(copyStartTimeMap[i], System.Globalization.CultureInfo.InvariantCulture));
									row["transaction_copy_duration"]    		  = String.Format("{0} hours, {1} minutes, {2} seconds", tranCopyDuration.Hours, tranCopyDuration.Minutes, tranCopyDuration.Seconds);		
									TimeSpan tranFilterCopyDuration               = DateTime.Parse(copyFilterEndTimeMap[i], System.Globalization.CultureInfo.InvariantCulture).Subtract(DateTime.Parse(copyFilterStartTimeMap[i], System.Globalization.CultureInfo.InvariantCulture));
									row["filter_copy_duration"]  			      = String.Format("{0} hours, {1} minutes, {2} seconds", tranFilterCopyDuration.Hours, tranFilterCopyDuration.Minutes, tranFilterCopyDuration.Seconds);
									row["transaction_staging_count"]  			  = copyTransCountMap[i];
									row["filter_staging_count"]  			      = copyFilterTransCountMap[i];
									row["inserted_transaction_count"]  			  = copyTransCountMap[i] - copyFilterTransCountMap[i];
									table.Rows.Add(row);

								}
								
								reportTableMap.Add("Arbiter Session Details",table);



							 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);
						} else{
                    
					    Console.WriteLine("Could not get Schema for destination table");
                        ArbiterCopyUtilLibrary.writeToLog("Could not get Schema for destination table");
                        Environment.Exit(0);

                    }



						}else{
								Console.WriteLine("Unable to connect to source database: "+ArbiterCopyUtilLibrary.sourceServer);
								ArbiterCopyUtilLibrary.writeToLog("Unable to connect to source database: "+ArbiterCopyUtilLibrary.sourceServer);
								Environment.Exit(0);

						}

						
						
                   }catch(Exception e){

                         Console.WriteLine("Error: " + e.ToString());
						  Console.WriteLine("Error Message: " + e.Message);
                         Console.WriteLine(e.StackTrace);
						 ArbiterCopyUtilLibrary.writeToLog("Error: " + e.ToString());
						 ArbiterCopyUtilLibrary.writeToLog("Error Message: " + e.Message);
						 Console.WriteLine(e.ToString());
                   }
				    // ArbiterCopyUtilLibrary.closeLogFile();
              }
           			  public static void initConnectionStrings(){
 				  
 				    sourceServerConnectionString    =  "Network Library=DBMSSOCN;Data Source=" +  ArbiterCopyUtilLibrary.sourceConnectionProps.getSourceServer() + ","+ArbiterCopyUtilLibrary.sourcePort+";database=" + ArbiterCopyUtilLibrary.sourceConnectionProps.getSourceDatabase()+ ";User id=" + ArbiterCopyUtilLibrary.sourceConnectionProps.getSourceUser()+ ";Password=" + ArbiterCopyUtilLibrary.sourceConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;Packet Size=8192;";     
                     destServerConnectionString     =  "Network Library=DBMSSOCN;Data Source=" +  ArbiterCopyUtilLibrary.destinationConnectionProps.getSourceServer() + ","+ArbiterCopyUtilLibrary.destinationPort+";database=" + ArbiterCopyUtilLibrary.destinationConnectionProps.getSourceDatabase()+ ";User id=" +  ArbiterCopyUtilLibrary.destinationConnectionProps.getSourceUser()+ ";Password=" + ArbiterCopyUtilLibrary.destinationConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;Packet Size=8192;";    
     				stagingConnectionString			=  "Network Library=DBMSSOCN;Data Source=" +  ArbiterCopyUtilLibrary.stagingConnectionProps.getSourceServer() + ","+ArbiterCopyUtilLibrary.stagingPort+";database=" + ArbiterCopyUtilLibrary.stagingConnectionProps.getSourceDatabase()+ ";User id=" +  ArbiterCopyUtilLibrary.stagingConnectionProps.getSourceUser()+ ";Password=" + ArbiterCopyUtilLibrary.stagingConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;Packet Size=8192;";    
				    connectionStringMap.Add("source",sourceServerConnectionString);
 					connectionStringMap.Add("destination",destServerConnectionString);
					connectionStringMap.Add("staging",stagingConnectionString);
  
			  }

 				public   void  runArbiterTransactionCopy(string sourceServer,  string bulkQuery, int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString         = connectionStringMap[sourceServer];
			 	arbiterCopyTables[tabInd]               = new DataTable();
				string  tableName                       = ArbiterCopyUtilLibrary.arbiterCopyTableNamePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType.ToLower()+"_"+tabInd.ToString();
				copyTableNameMap.Add(tabInd,tableName);
				try{
                lock (locker)
                {	Console.WriteLine("Running fetch script for table: "+tabInd.ToString());

					if( ArbiterCopyUtilLibrary.fs.BaseStream != null){
							ArbiterCopyUtilLibrary.writeToLog("Running fetch script for table: " + tabInd.ToString()+"\n"+bulkQuery);
					}
				}
				copyStartTimeMap.Add(tabInd,DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                bulkCopyDataFromRemoteServer(bulkQuery, tableName);
				copyEndTimeMap.Add(tabInd,DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

			}catch(Exception e){

						
					
								Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+arbiterCopyTables[tabInd].ToString()+". Error: " + e.Message);
								Console.WriteLine(e.StackTrace);
								ArbiterCopyUtilLibrary.writeToLog("Error while running bulk insert script "+bulkQuery+": " + e.Message);
								ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
								Console.WriteLine(e.ToString());
								emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert script "+bulkQuery+": " + e.Message);
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);
					
						}
				}
			
			  public static void wait(){

				  int activeThreadCount               =  0;
				  HashSet<int> completedThreadSet     =  new  HashSet<int>();
				  int shouldWait                      =  1;
				  int  skippedCount					  =  0;
				  while(shouldWait!= 0){
					  		 shouldWait               =  0;
						     skippedCount			  =  0;
                             activeThreadCount        =  0;
							 foreach(Thread arbCopyThread  in arbiterCopyThreads){
								
								if(arbCopyThread.IsAlive){
										++activeThreadCount;
                      				 
								}
						 	}

							 	 foreach(Thread arbCopyFilterThread  in arbiterCopyFilterThreads){
						
								if(arbCopyFilterThread.IsAlive){
										++activeThreadCount;
                      				 
								}
						 	}
            
                           if(completedThreadSet.Count !=arbiterCopyThreads.Length){
              
					        for (int i =  0;  i< arbiterCopyThreads.Length; i++ ) {
						
								if( !arbiterCopyThreads[i].IsAlive &&  !arbiterCopyFilterThreads[i].IsAlive){
									if(!completedThreadSet.Contains(i)){
											completedThreadSet.Add(i);
											Thread exportThread =  	new Thread(()=> {
										        exportDataToDestinationTable(i);
									
											});
											exportThread.Start();
											exportThread.Join();
									}
								} else{
									++shouldWait;
								}
							}

							if(shouldWait>0){
									Console.WriteLine("Waiting for  " + ArbiterCopyUtilLibrary.WAIT_INTERVAL.ToString());
								Thread.Sleep(ArbiterCopyUtilLibrary.WAIT_INTERVAL);  
							}

							Console.WriteLine("Current completed thread count: "+completedThreadSet.Count.ToString());
							Console.WriteLine("Current running count: "+activeThreadCount.ToString());
							ArbiterCopyUtilLibrary.writeToLog("Current completed thread count: " + completedThreadSet.Count.ToString());
							ArbiterCopyUtilLibrary.writeToLog("Current running thread count: "+activeThreadCount.ToString());
						} else{


							for (int i =  0;  i< arbiterCopyThreads.Length; i++ ) {

								if( !arbiterCopyThreads[i].IsAlive &&  !arbiterCopyFilterThreads[i].IsAlive){
										if(!completedThreadSet.Contains(i)){

											completedThreadSet.Add(i);
											
											Thread exportThread =  	new Thread(()=> {
										
													 exportDataToDestinationTable(i);
									
											});
											exportThread.Start();
											exportThread.Join();
											
									     }else{

											 ++skippedCount;

										 }
								} else {
									++shouldWait;
								}
							}
						}
						 Console.WriteLine("Current completed thread count: " + completedThreadSet.Count.ToString());
                		Console.WriteLine("Current running count: " +activeThreadCount.ToString());
               		    ArbiterCopyUtilLibrary.writeToLog("Current completed thread count: " + completedThreadSet.Count.ToString());
                		ArbiterCopyUtilLibrary.writeToLog("Current running thread count: " + activeThreadCount.ToString());
						if(skippedCount==arbiterCopyThreads.Length)break;
				  }



               


            }
			

			  public static void createSQLTableFromDataTable(string destTableName, DataTable  reportTable, string connectionString){
                
			      StringBuilder destinationTableCreateBuilder   =  new  StringBuilder();
				  destinationTableCreateBuilder.Append( "IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].["+destTableName+"]') AND type in (N'U')) BEGIN  DROP TABLE ["+destTableName+"] END");
				  destinationTableCreateBuilder.Append(" CREATE TABLE ["+destTableName+"] ( ");


				  string colDataType  = "";
				  foreach(DataColumn col in reportTable.Columns){
					
					
									if(col.ColumnName.Contains("structured_data") || col.ColumnName.Contains("icc_data")){
										colDataType = "VARCHAR(MAX)";
									}else {
										if(col.DataType.Name.ToString()=="Boolean")
										{
										colDataType = "BIT";
										}
										if(col.DataType.Name.ToString()=="Byte")
										{
										colDataType = "VARBINARY(MAX)";
										}
										if(col.DataType.Name.ToString()=="Char")
										{
										colDataType = "CHAR(1)";
										}
										if(col.DataType.Name.ToString()=="DateTime")
										{
										colDataType = "DATETIME";
										}
										if(col.DataType.Name.ToString()=="Decimal")
										{
										colDataType = "DECIMAL(20,2)";
										}
										if(col.DataType.Name.ToString()=="Double")
										{
										colDataType = "FLOAT";
										}
										if(col.DataType.Name.ToString()=="Int16")
										{
										colDataType = "INT";
										}
										if(col.DataType.Name.ToString()=="Int32")
										{
										colDataType = "INT";
										}
										if(col.DataType.Name.ToString()=="Int64")
										{
										colDataType = "BIGINT";
										}
										if(col.DataType.Name.ToString()=="SByte")
										{
										colDataType = "VARBINARY(MAX)";
										}
										if(col.DataType.Name.ToString()=="Single")
										{
										colDataType = "VARBINARY(MAX)";
										}
										if(col.DataType.Name.ToString()=="String")
										
										{
											colDataType = "VARCHAR(300)";
										}
										if(col.DataType.Name.ToString()=="TimeSpan")
										
										{
											colDataType = "BIT";
										}
										if(col.DataType.Name.ToString()=="UInt16")
										
										{
											colDataType = "DATETIME";
										}
										if(col.DataType.Name.ToString()=="UInt32")
										
										{
											colDataType = "BIGINT";
										}
										if(col.DataType.Name.ToString()=="UInt64")
										
										{
											colDataType = "BIGINT";
										}
									}

                                               destinationTableCreateBuilder.Append(col.ColumnName);
											   destinationTableCreateBuilder.Append("\t");
											   destinationTableCreateBuilder.Append(colDataType);
											   destinationTableCreateBuilder.Append(",");
									
								}
									destinationTableCreateBuilder.Length--;
									destinationTableCreateBuilder.Append(" );");
									executeScript(destinationTableCreateBuilder.ToString(),  connectionString);
			  }
			  public bool serverIsReachable(string connectionStr){
				  bool isReachable   = false;

				   try{
					
                    using (SqlConnection serverConnection =  new SqlConnection(connectionStr)){
                                 serverConnection.Open();
								 isReachable   = true;
					}
                         
				   }catch(Exception e){
								   ArbiterCopyUtilLibrary.writeToLog("Error while connecting to server: " + e.Message);
								   ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
								   ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								   Console.WriteLine(e.ToString());
                        }
						return isReachable;
			  }
			  
              public  static  DataTable  getDataFromSQL(string theScript, string targetConnectionString ){
	         
                DataTable  dt = new DataTable();

                try{
					
                    using (SqlConnection serverConnection =  new SqlConnection(targetConnectionString)){
                    SqlCommand cmd = new SqlCommand(theScript, serverConnection);
                    Console.WriteLine("Executing script: "+theScript);
                    ArbiterCopyUtilLibrary.writeToLog("Executing script: "+theScript);
                    cmd.CommandTimeout =0;
                    serverConnection.Open();
                    SqlDataReader  reader = cmd.ExecuteReader();
                    dt.Load(reader);	
                    cmd.Dispose();
					
                   }
                } catch(Exception e){
					
					if(e.Message.ToLower().Contains("transport")){
						
						 Console.WriteLine("Error while running script: "+theScript+". The error is: "+e.Message);
						 Console.WriteLine("The fetch session would now be restarted");
						 ArbiterCopyUtilLibrary.writeToLog("Error while running fetch script: "+theScript+". The error is: "+e.Message);
						 ArbiterCopyUtilLibrary.writeToLog("The data fetch session would now be restarted");
						 getDataFromSQL( theScript,  targetConnectionString );
						 ArbiterCopyUtilLibrary.writeToLog(e.ToString());
					     emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+theScript+". The error is: "+e.Message);
						 emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
				
					} else {
						
						Console.WriteLine("Error while running script: " + e.Message);
						Console.WriteLine(e.StackTrace);
						 ArbiterCopyUtilLibrary.writeToLog(e.ToString());
						 ArbiterCopyUtilLibrary.writeToLog("Error while running script: " + e.Message);
						 ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
						 Console.WriteLine(e.ToString());
						 ArbiterCopyUtilLibrary.writeToLog(e.ToString());
						emailError.AppendLine("<div style=\"color:red\">Error while running script: " + e.Message);
						emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
						 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);
								
						 }
					}
					return  dt;
			  }
			  


             public System.Data.DataTable   getDataFromSourceDatabase (string script){
                            System.Data.DataTable dt = new DataTable();
                            try{

                                using (SqlConnection serverConnection =  new SqlConnection(connectionStringMap["source"])){
									SqlCommand cmd = new SqlCommand(script, serverConnection);
									ArbiterCopyUtilLibrary.writeToLog("Executing script: "+script+" on source database.");
									cmd.CommandTimeout =0;
									serverConnection.Open();
									SqlDataReader  reader = cmd.ExecuteReader();
									dt.Load(reader);	
									cmd.Dispose();
                        }
                        }catch(Exception e){
								   ArbiterCopyUtilLibrary.writeToLog("Error while running script: " + e.Message);
								   ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
								   ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								   Console.WriteLine(e.ToString());
                        }
                        return dt;
                }

              				public static void  executeScript( string script, string  targetConnectionString){

						try{
							using (SqlConnection serverConnection =  new SqlConnection(targetConnectionString)){
									SqlCommand cmd = new SqlCommand(script, serverConnection);
									Console.WriteLine("Executing script: "+script);
									ArbiterCopyUtilLibrary.writeToLog("Executing script: "+script);
									cmd.CommandTimeout =0;
									serverConnection.Open();
									cmd.ExecuteNonQuery();
							}
						}catch(Exception e){

									 Console.WriteLine("Error while running script: " + e.Message);
									 Console.WriteLine(e.StackTrace);
									 ArbiterCopyUtilLibrary.writeToLog("Error while running script: " + e.Message);
									 ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
									 ArbiterCopyUtilLibrary.writeToLog(e.ToString());
									  Console.WriteLine(e.ToString());
									 emailError.AppendLine("<div style=\"color:red\">Error while running script: " + e.Message);
									 emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
									  if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);

							}
				}


				
			
			 public static long getAggregateFromTable (string aggr, string tableName,string columnName, string server){
				  
				   string script                       = "SELECT  aggr_val  = "+aggr+"("+columnName+") FROM "+tableName+" WITH (NOLOCK)";
				   DataTable aggregateVal 			   = getDataFromSQL(script, connectionStringMap[server]);
				   return string.IsNullOrEmpty(aggregateVal.Rows[0]["aggr_val"].ToString())?0: long.Parse(aggregateVal.Rows[0]["aggr_val"].ToString());    

			}	
             public static void bulkCopyTableData(string server, DataTable dTab , string  destTable){

                        string  connectionStr = connectionStringMap[server];
                    
                        try{

                        using (SqlConnection bulkCopyConnection =  new SqlConnection(connectionStr)){
                            
                            using (var bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.TableLock|SqlBulkCopyOptions.UseInternalTransaction | SqlBulkCopyOptions.KeepNulls,null))
                            {
                                bulkCopyConnection.Open();
                                foreach (DataColumn col in dTab.Columns){
									
											bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
									
                                }						
								bulkCopy.BulkCopyTimeout = 0; 
								bulkCopy.BatchSize =  ArbiterCopyUtilLibrary.batchSize;
								bulkCopy.DestinationTableName = destTable;
								bulkCopy.WriteToServer(dTab);
                            }
                         }	
                    }catch(Exception e){
                       	
								Console.WriteLine("Error while running bulk insert: " + e.Message);
								Console.WriteLine(e.StackTrace);
								ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								 Console.WriteLine(e.ToString());
								ArbiterCopyUtilLibrary.writeToLog("Error while running bulk insert: " + e.Message);
								ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
								emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert: " + e.Message);
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);
						 
					}

                    
                }


	    public   static   void  runArbiterTransactionCopyFilter(string destinationServer,  string bulkQuery, int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString         = connectionStringMap[destinationServer];
			 	arbiterCopyFilterTables[tabInd]         = new DataTable();
				string  tableName                       = ArbiterCopyUtilLibrary.arbiterCopyFilterTablePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType.ToLower()+"_"+tabInd.ToString();
				 if(!copyFilterTableNameMap.ContainsKey(tabInd)){
					 copyFilterTableNameMap.Add(tabInd,tableName);
				 }
				try{
                lock (locker)
                {	Console.WriteLine("Running fetch script for table: "+tabInd.ToString());

					if( ArbiterCopyUtilLibrary.fs.BaseStream != null){
							ArbiterCopyUtilLibrary.writeToLog("Running fetch script for table: " + tabInd.ToString()+"\n"+bulkQuery);
					}
				}
				
					 if(!copyFilterStartTimeMap.ContainsKey(tabInd)){
						 copyFilterStartTimeMap.Add(tabInd,DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")); 
					 }         
                bulkCopyToStagingServer(bulkQuery, tableName, destServerConnectionString,stagingConnectionString, true, tabInd);
					 if(!copyFilterEndTimeMap.ContainsKey(tabInd)){
						copyFilterEndTimeMap.Add(tabInd,DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
					 }

			 }catch(Exception e){
						Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+arbiterCopyFilterTables[tabInd].ToString()+". Error: " + e.Message);
						Console.WriteLine(e.StackTrace);
						ArbiterCopyUtilLibrary.writeToLog("Error while running bulk insert script "+bulkQuery+": " + e.Message);
						ArbiterCopyUtilLibrary.writeToLog(e.ToString());
						ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
						Console.WriteLine(e.ToString());
						emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert script "+bulkQuery+": " + e.Message);
						emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
						 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);
			
						}
				}
        public static void bulkCopyDataFromRemoteServer( string copyScript, string destTable)
        {

            createSQLTableFromDataTable(destTable, schemaTable, stagingConnectionString);
           try
            {
                using (SqlConnection serverConnection = new SqlConnection(sourceServerConnectionString))
                {
                   
                    SqlCommand cmd = new SqlCommand(copyScript , serverConnection);

                    cmd = new SqlCommand(copyScript, serverConnection);
                    Console.WriteLine("Executing script: " + copyScript);
                    ArbiterCopyUtilLibrary.writeToLog("Executing script: " + copyScript);
                    cmd.CommandTimeout = 0;
                    if (serverConnection != null && serverConnection.State == ConnectionState.Closed)
                    {

                        serverConnection.Open();
                    }
                    SqlDataReader reader = cmd.ExecuteReader();
                    using (SqlConnection bulkCopyConnection = new SqlConnection(stagingConnectionString))
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null))
                        {
                            bulkCopyConnection.Open();
                            bulkCopy.BulkCopyTimeout	  = 0;
                            bulkCopy.BatchSize 			  = ArbiterCopyUtilLibrary.batchSize;
							bulkCopy.EnableStreaming      = false;
                            bulkCopy.DestinationTableName = destTable;
                            bulkCopy.WriteToServer(reader);
                        }
                    }
                    reader.Close();
                }
            }
            catch (Exception e)
            {

					if(!e.Message.ToLower().Contains("filegroup")){
								Console.WriteLine("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								Console.WriteLine("The data fetch session would now be restarted");
								ArbiterCopyUtilLibrary.writeToLog("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								ArbiterCopyUtilLibrary.writeToLog("The data fetch session would now be restarted");
								ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								Console.WriteLine(e.ToString());
								bulkCopyDataFromRemoteServer( copyScript,  destTable); 
								emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {

							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							ArbiterCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
							 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);

					}
               
            }
        }

 public static void bulkCopyToStagingServer( string copyScript, string destTable, string connectionString,string stageConStr,  bool createTable, int tableIndex)
        {
             DataTable schmTable   =getColumns(copyScript,connectionString);
            if(createTable) createSQLTableFromDataTable(destTable, schmTable, stageConStr);
           try
            {
                using (SqlConnection serverConnection = new SqlConnection(connectionString))
                {
                   
                    SqlCommand cmd = new SqlCommand(copyScript , serverConnection);

                    cmd = new SqlCommand(copyScript, serverConnection);
                    Console.WriteLine("Executing script: " + copyScript);
                    ArbiterCopyUtilLibrary.writeToLog("Executing script: " + copyScript);
                    cmd.CommandTimeout = 0;
                    if (serverConnection != null && serverConnection.State == ConnectionState.Closed)
                    {

                        serverConnection.Open();
                    }
                    SqlDataReader reader = cmd.ExecuteReader();
                    using (SqlConnection bulkCopyConnection = new SqlConnection(stageConStr))
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null))
                        {
                            bulkCopyConnection.Open();
                            bulkCopy.BulkCopyTimeout	  = 0;
                            bulkCopy.BatchSize 			  = ArbiterCopyUtilLibrary.batchSize;
							bulkCopy.EnableStreaming      = false;
                            bulkCopy.DestinationTableName = destTable;
                            bulkCopy.WriteToServer(reader);
                        }
                    }
                    reader.Close();
                }
            }
            catch (Exception e)
            {

					if(!e.Message.ToLower().Contains("filegroup")){
								Console.WriteLine("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								Console.WriteLine("The data fetch session would now be restarted");
								ArbiterCopyUtilLibrary.writeToLog("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								ArbiterCopyUtilLibrary.writeToLog("The data fetch session would now be restarted");
								ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								Console.WriteLine(e.ToString());
								 runArbiterTransactionCopyFilter("destination",copyFilterScript[tableIndex],tableIndex);
								 string  filterTableIndex    		     =  "IF((SELECT  1  FROM sys.indexes WHERE name='[ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"] ' AND object_id = OBJECT_ID('dbo.["+copyFilterTableNameMap[tableIndex]+"]' )) = 1) BEGIN  DROP INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON  dbo.["+copyFilterTableNameMap[tableIndex]+"]  END"+
							                                        	     " CREATE INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON ["+copyFilterTableNameMap[tableIndex]+"] (["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]);";
								executeScript(filterTableIndex, connectionStringMap["staging"]);
								bulkCopyToStagingServer( copyScript, destTable,connectionString,stageConStr,createTable,tableIndex); 
								emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {
							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							ArbiterCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
							 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);

					}
               
            }
        }

        public static DataTable getColumns(string sqlScript, string  connectionString)
        {
		   ArrayList columnList     =   new ArrayList();
		   DataTable schemaTable =  new DataTable();
           try{
            sqlScript =sqlScript.Contains("#")? sqlScript:"SET FMTONLY ON \n" + sqlScript;
             connectionString = connectionString.Replace("Network Library=DBMSSOCN", "Provider=SQLOLEDB");
            using (OleDbDataAdapter oda = new OleDbDataAdapter(sqlScript, connectionString))
            {
                oda.SelectCommand.CommandTimeout = 0;
                DataSet ds = new DataSet();
                oda.Fill(ds);
                schemaTable = ds.Tables[0];
            }
			  
			 }catch (Exception e)
            {

                Console.WriteLine("Error getting  table schema from server with script:\n"+sqlScript +". \nError " + e.Message);
                Console.WriteLine(e.StackTrace);
                ArbiterCopyUtilLibrary.writeToLog("Error getting  table schema from server with script:\n" + sqlScript + ". \nError " + e.Message);
                ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
             


            }

            return schemaTable;
        }
     public static  void  exportDataToDestinationTable( int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString      = connectionStringMap["destination"];
				string   stagingTableName            = ArbiterCopyUtilLibrary.arbiterCopyTableNamePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType.ToLower()+"_"+tabInd.ToString();
				string   filterTableName             = ArbiterCopyUtilLibrary.arbiterCopyFilterTablePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType.ToLower()+"_"+tabInd.ToString();
				
				string  stagingTableIndex     		 = "IF((SELECT  1  FROM sys.indexes WHERE name='[ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"] ' AND object_id = OBJECT_ID('dbo.["+stagingTableName+"]' )) = 1) BEGIN  DROP INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON  dbo.["+stagingTableName+"]   END "+
				                                       " CREATE INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON ["+stagingTableName+"] (["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]);";
				string  filterTableIndex    		 = "IF((SELECT  1  FROM sys.indexes WHERE name='[ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"] ' AND object_id = OBJECT_ID('dbo.["+filterTableName+"]' )) = 1) BEGIN  DROP INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON  dbo.["+filterTableName+"]   END "+
				                           			   " CREATE INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON ["+filterTableName+"] (["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]);";
			
				executeScript(filterTableIndex,  connectionStringMap["staging"]);
				executeScript(stagingTableIndex, connectionStringMap["staging"]);
				
				string  bulkInsertScript             = "SELECT  * FROM  ["+stagingTableName+"] WITH (NOLOCK, INDEX=ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+")  WHERE  ["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"] NOT IN (SELECT ["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  FROM  ["+filterTableName+"]   WITH (NOLOCK, INDEX=ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"))";
			  try{
				  

                bulkCopyDataToDestinationServer(bulkInsertScript, ArbiterCopyUtilLibrary.destinationTable, connectionStringMap["destination"],tabInd);
                
			}catch(Exception e){

						
					
								Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+arbiterCopyTables[tabInd].ToString()+". Error: " + e.Message);
								Console.WriteLine(e.StackTrace);
								ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
								Console.WriteLine(e.ToString());
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);
					
						}
				}
		        public static void bulkCopyDataToDestinationServer( string copyScript, string destTable, string connectionStr, int tableIndex)
        {

                         
           try
            {
                using (SqlConnection serverConnection = new SqlConnection(stagingConnectionString))
                {
                    
					DataTable schmTable = getColumns(copyScript, connectionStr);
					ArrayList columns   = new ArrayList();

					foreach (DataColumn col in schmTable.Columns){
				     columns.Add(col.ColumnName);
					}

                    SqlCommand cmd = new SqlCommand(copyScript , serverConnection);

                    cmd = new SqlCommand(copyScript, serverConnection);
                    Console.WriteLine("Executing script: " + copyScript);
                    ArbiterCopyUtilLibrary.writeToLog("Executing script: " + copyScript);
                    cmd.CommandTimeout = 0;
                    if (serverConnection != null && serverConnection.State == ConnectionState.Closed)
                    {

                        serverConnection.Open();
                    }
                    SqlDataReader reader = cmd.ExecuteReader();
                    using (SqlConnection bulkCopyConnection = new SqlConnection(connectionStr))
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.UseInternalTransaction, null))
                        {

                            bulkCopyConnection.Open();
                            bulkCopy.BulkCopyTimeout	  = 0;
							foreach(string  col in  columns){
								bulkCopy.ColumnMappings.Add(col, col);
							}
                            bulkCopy.BatchSize 			  = ArbiterCopyUtilLibrary.batchSize;

							/* 
							bulkCopy.NotifyAfter = 1;
						    bulkCopy.SqlRowsCopied += (sender, args) => {
                                  ++totalRecordCount;
							};   
							*/
                            bulkCopy.DestinationTableName = destTable;
							  lock (locker){
                           		 bulkCopy.WriteToServer(reader);
								}
                        }

						
                    }
                    reader.Close();
                }
			
            }
            catch (Exception e)
            {

					if(!e.Message.ToLower().Contains("filegroup")){
								Console.WriteLine("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								Console.WriteLine("The data fetch session would now be restarted");
								ArbiterCopyUtilLibrary.writeToLog("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								ArbiterCopyUtilLibrary.writeToLog("The data fetch session would now be restarted");
								ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								Console.WriteLine(e.ToString());
                                runArbiterTransactionCopyFilter("destination",copyFilterScript[tableIndex],tableIndex);
								 string  filterTableIndex    		     =  "IF((SELECT  1  FROM sys.indexes WHERE name='[ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"] ' AND object_id = OBJECT_ID('dbo.["+copyFilterTableNameMap[tableIndex]+"]' )) = 1) BEGIN  DROP INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON  dbo.["+copyFilterTableNameMap[tableIndex]+"]  END"+
							                                        	     " CREATE INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON ["+copyFilterTableNameMap[tableIndex]+"] (["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]);";
								executeScript(filterTableIndex, connectionStringMap["staging"]);
								bulkCopyDataToDestinationServer( copyScript,  destTable,connectionStr, tableIndex); 
								emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {

							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							ArbiterCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
							 if(ArbiterCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);

					}
               
            }
        }

        public static void getSchemaTable(string sqlScript)
        {
           try{
            sqlScript =sqlScript.Contains("#")? sqlScript:"SET FMTONLY ON \n" + sqlScript;
            string targetConnectionString = sourceServerConnectionString.Replace("Network Library=DBMSSOCN", "Provider=SQLOLEDB");
            using (OleDbDataAdapter oda = new OleDbDataAdapter(sqlScript, targetConnectionString))
            {
                oda.SelectCommand.CommandTimeout = 0;
                DataSet ds = new DataSet();
                oda.Fill(ds);
                schemaTable = ds.Tables[0];
            }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error getting  table schema from server with script:\n"+sqlScript +". \nError " + e.Message);
                Console.WriteLine(e.StackTrace);
                ArbiterCopyUtilLibrary.writeToLog("Error getting  table schema from server with script:\n" + sqlScript + ". \nError " + e.Message);
                ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
             
            }


        }

        public static void getFilterTableSchema(string sqlScript)
        {
           try{
            sqlScript =sqlScript.Contains("#")? sqlScript:"SET FMTONLY ON \n" + sqlScript;
            string targetConnectionString = destServerConnectionString.Replace("Network Library=DBMSSOCN", "Provider=SQLOLEDB");
            using (OleDbDataAdapter oda = new OleDbDataAdapter(sqlScript, targetConnectionString))
            {
                oda.SelectCommand.CommandTimeout = 0;
                DataSet ds = new DataSet();
                oda.Fill(ds);
                filterTableSchema = ds.Tables[0];
            }
            }
            catch (Exception e)
            {

                Console.WriteLine("Error getting filter  table schema from server with script:\n"+sqlScript +". \nError " + e.Message);
                Console.WriteLine(e.StackTrace);
                ArbiterCopyUtilLibrary.writeToLog("Error getting filter table schema from server with script:\n" + sqlScript + ". \nError " + e.Message);
                ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
             
            }


        }

public  static void sendMailNotification(Dictionary<string, DataTable> dTableMap){

	            Console.WriteLine("Sending Notification... ");
				ArbiterCopyUtilLibrary.writeToLog("Sending Notification... ");
				emailBody.AppendLine("<div style=\"color:black\">Hi, All.</div>");
				emailBody.AppendLine("<div style=\"color:black\">\n</div>");
				emailBody.AppendLine("<div style=\"color:black\">Trust this meets you well</div>");
				emailBody.AppendLine("<div style=\"color:black\">\n</div>");
				emailBody.AppendLine("<div style=\"color:black\">Please see report for "+ArbiterCopyUtilLibrary.arbiterCopyType+" Copy Session below: </div>");
                MailMessage message = new MailMessage();

				foreach (var address in ArbiterCopyUtilLibrary.toAddress.Split(new [] {ArbiterCopyUtilLibrary.emailSeparator}, StringSplitOptions.RemoveEmptyEntries)){
						if(!string.IsNullOrWhiteSpace(address)){
									message.To.Add(address);   	
						}
				}
				foreach (var address in ArbiterCopyUtilLibrary.ccAddress.Split(new [] {ArbiterCopyUtilLibrary.emailSeparator}, StringSplitOptions.RemoveEmptyEntries)){
					if(!string.IsNullOrWhiteSpace(address)){
					message.CC.Add(address);   	
					}
				}
				foreach (var address in ArbiterCopyUtilLibrary.bccAddress.Split(new [] {ArbiterCopyUtilLibrary.emailSeparator}, StringSplitOptions.RemoveEmptyEntries)){
							if(!string.IsNullOrWhiteSpace(address)){
										message.Bcc.Add(address);   	
							}
				}
				Console.WriteLine("Sending Notification... ");
				message.From = new MailAddress(ArbiterCopyUtilLibrary.fromAddress);				
				message.Subject = "Arbiter Transaction Copy Report for "+ArbiterCopyUtilLibrary.arbiterCopyType+" at  "+DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");
				message.IsBodyHtml = true;
		
				emailBody.AppendLine("<style type=\"text/css\">");
				emailBody.AppendLine("table.gridtable {");
				emailBody.AppendLine("	font-family:"+ArbiterCopyUtilLibrary.emailFontFamily+";");
				emailBody.AppendLine("	font-size:"+ArbiterCopyUtilLibrary.emailFontSize+";");
				emailBody.AppendLine("	color:"+ArbiterCopyUtilLibrary.borderColour+";");
				emailBody.AppendLine("	border-width:"+ArbiterCopyUtilLibrary.borderWidth+";");
				emailBody.AppendLine("	border-color: "+ArbiterCopyUtilLibrary.borderColour+";");
				emailBody.AppendLine("	border-collapse: collapse;");
				emailBody.AppendLine("}");
				emailBody.AppendLine("table.gridtable th {");
				emailBody.AppendLine("	border-width: "+ArbiterCopyUtilLibrary.borderWidth+";");
				emailBody.AppendLine("	padding: 8px;");
				emailBody.AppendLine("	border-style: solid;");
				emailBody.AppendLine("	border-color:"+ArbiterCopyUtilLibrary.borderColour+";");
				emailBody.AppendLine("	background-color:"+ArbiterCopyUtilLibrary.headerBgColor+";");
				emailBody.AppendLine("}");
				emailBody.AppendLine("table.gridtable td {");
				emailBody.AppendLine("	border-width: 1px;");
				emailBody.AppendLine("	padding: 8px;");
				emailBody.AppendLine("	border-style: solid;");
				emailBody.AppendLine("	border-color: "+ArbiterCopyUtilLibrary.borderColour+";");
				emailBody.AppendLine("}");
				emailBody.AppendLine("</style>");
				
				

				foreach(KeyValuePair<string, DataTable>  reportTabMap in reportTableMap){
					 emailBody.AppendLine("<div>\n</div>");
				
				     emailBody.AppendLine("<div><hr/></div>");
					 emailBody.AppendLine("<div justify=\"left\"><table class=\"gridtable\">");
					 emailBody.AppendLine("<thead>");
				     emailBody.AppendLine("<caption style=\"color:gray\" justify=\"left\">"+reportTabMap.Key+"</caption>");
					 foreach (DataColumn col in reportTabMap.Value.Columns){
					 	emailBody.AppendLine("<th>"+col.ColumnName+"</th>");
					 }
					 
					emailBody.AppendLine("</thead>");
					emailBody.AppendLine("<tbody>");
					
					int k = 0;
			        foreach (DataRow row in reportTabMap.Value.Rows) {
                      if(k%2!=0){
								emailBody.AppendLine("<tr style=\"background-color:#ffffff\"> ");   // <td>"+row["INDEX_NO"]+"</td><td>"+row["PARAMETER"]+"</td><td>"+row["VALUE"]+"</td></tr>");
					    } else{
								emailBody.AppendLine("<tr style=\"background-color:"+ArbiterCopyUtilLibrary.alternateRowColour+"\">"); //<td>"+row["INDEX_NO"]+"</td><td>"+row["PARAMETER"]+"</td><td>"+row["VALUE"]+"</td></tr>");
					  }
					
					  foreach(DataColumn dCol in  reportTabMap.Value.Columns){
						  
						 emailBody.AppendLine("<td>"+ row[dCol.ToString()].ToString()+"</td>");
						   
					  }
					  emailBody.AppendLine("</tr>");
				      ++k;
			        }
					 emailBody.AppendLine("</tbody>");
			 emailBody.AppendLine("</table></div>");
				 }
			    
 		   emailBody.AppendLine("<div><hr/></div>");
			if(!string.IsNullOrWhiteSpace(emailError.ToString())){
				emailBody.AppendLine("<div><strong> Error List </strong></div>");
				emailBody.AppendLine("<div>\n</div>");
				emailBody.AppendLine("<div>\n</div>");
				emailBody.AppendLine(emailError.ToString());
			}
			emailBody.AppendLine("<div>\n</div>");
				emailBody.AppendLine("<div>\n</div>");
			emailBody.AppendLine("Thank you.");
			
	        message.Body = emailBody.ToString();
			SmtpClient smtpClient = new SmtpClient();
			smtpClient.UseDefaultCredentials = true;

			smtpClient.Host = ArbiterCopyUtilLibrary.smtpServer;
			smtpClient.Port = Int32.Parse(ArbiterCopyUtilLibrary.smtpPort.ToString());
			smtpClient.EnableSsl = ArbiterCopyUtilLibrary.isSSLEnabled;
			smtpClient.Credentials = new System.Net.NetworkCredential(ArbiterCopyUtilLibrary.sender, ArbiterCopyUtilLibrary.senderPassword);
			smtpClient.Send(message);

		}

			public static void cleanUp(){

                    string  filterTableDeleteScript   =""; 
					string  stagingTableDeleteScript  = "";
                    for(int i=0; i<numberOfTables; i++){
					   stagingTableDeleteScript            = " IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].["+copyTableNameMap[i]+"]') AND type in (N'U')) BEGIN  DROP TABLE ["+copyTableNameMap[i]+"] END";
					   filterTableDeleteScript             = " IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].["+copyFilterTableNameMap[i]+"]') AND type in (N'U')) BEGIN  DROP TABLE ["+copyFilterTableNameMap[i]+"] END";
					   Console.WriteLine("Removing filter table: "+copyFilterTableNameMap[i]+"...");
					   ArbiterCopyUtilLibrary.writeToLog("Removing filter table: "+copyFilterTableNameMap[i]+"...");
					   executeScript(filterTableDeleteScript, connectionStringMap["staging"]);				
					   Console.WriteLine("Removing staging table: "+copyTableNameMap[i]+"...");
					   ArbiterCopyUtilLibrary.writeToLog("Removing staging table: "+copyTableNameMap[i]+"...");
					  executeScript(stagingTableDeleteScript, connectionStringMap["staging"]);
					
					}
					
					
					
				}
        public static void Main (string[] args){
				
				 string configFile 		= ""; 

					try {	
						for(int i =0; i< args.Length; i++){
							
							if (args[0].ToLower()=="-h" ||args[0].ToLower()=="help" || args[0].ToLower()=="/?" || args[0].ToLower()=="?" ){
								
								Console.WriteLine(" This application automates and optimizes the generation of Reports");
								Console.WriteLine(" Usage: ");	
								Console.WriteLine(" -c: This parameter is used to specify the configuration file to be used.");
                        		Console.WriteLine(" -s: This parameter is used to start the webserver.");
                       		    Console.WriteLine(" -h: This parameter is used to print this help message.");	

																
						   } else{
                    		
                           if(args[i].ToLower()=="-c" && (args[(i+1)] != null && args[(i+1)].Length!=0)){
								configFile =  args[(i+1)];	
					     	} 
					}   
								
							
						}
		
			
					 if(string.IsNullOrEmpty(configFile)){
						 
						 new  ArbiterCopy();
						 
					 }else {					
							new  ArbiterCopy(configFile);
					 }
					
					}catch(Exception e) {
					   
					   Console.WriteLine(e.Message);

					   Console.WriteLine(e.StackTrace);
                	 
					
					}
				

                }

            }

}