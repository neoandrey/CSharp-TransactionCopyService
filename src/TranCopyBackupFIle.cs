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

namespace TranCopyService{
            public class  TranCopy {

                 internal  static System.Data.DataTable[]  	       tranCopyTables;
				 internal  static Thread[]						   tranCopyThreads;


                 internal  static System.Data.DataTable[]  	       tranCopyFilterTables;
				 internal  static Thread[]						   tranCopyFilterThreads;            
		         internal  static Dictionary<string, string>       connectionStringMap     			    = new Dictionary<string,string>();	
			     internal  static Dictionary<string, string>       partitionSizeIntervalMap   			= new Dictionary<string,string>();	    
				 internal  static string                           sourceServerConnectionString;   
				 internal  static string                           destServerConnectionString;

				  internal  static string                          stagingConnectionString;
                 internal  static int 							   datePartitionInterval			    = 0;
				 internal  static Dictionary<int,Thread>           tranCopyThreadMap                 = new Dictionary<int,Thread>();

				  internal  static Dictionary<int,Thread>          tranCopyFilterThreadMap           = new Dictionary<int,Thread>();
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

				

                   


				 public TranCopy(){
						new  TranCopyUtilLibrary();
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
				
				foreach (PropertyInfo prop in TranCopyUtilLibrary.tranConfig.GetType().GetProperties())
				{
					
					if(null!=prop.GetValue(TranCopyUtilLibrary.tranConfig) && null!= prop.Name){
						row = table.NewRow();
						row["parameter_name"]  = prop.Name;
						if(prop.Name.ToString().Trim()=="staging_server" && prop.GetValue(TranCopyUtilLibrary.tranConfig,  null).ToString().Trim()=="localhost" ){
							row["parameter_value"] = "172.25.15.49";
							
						}else  if(prop.Name.ToString().Trim()=="destination_table_column_order" ){
		
							row["parameter_value"] =  string.Join(",", TranCopyUtilLibrary.destinationTableColumnOrder);
						}else{	
								row["parameter_value"] = string.IsNullOrWhiteSpace(prop.GetValue(TranCopyUtilLibrary.tranConfig,  null).ToString())?"NULL":prop.GetValue(TranCopyUtilLibrary.tranConfig,  null).ToString();						
						}
						TranCopyUtilLibrary.writeToLog(prop.Name+": "+row["parameter_value"]);
						table.Rows.Add(row);
					}
						
				}
	
				reportTableMap.Add("Tran Copy Session Parameters",table);


						startTranCopy();
            	
				 }

                public TranCopy(string  config){
				  
				
					string  nuConfig   = config.Contains("\\\\")? config:config.Replace("\\", "\\\\");
					if(File.Exists(nuConfig)){
					new  TranCopyUtilLibrary(nuConfig);
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
				
				foreach (PropertyInfo prop in TranCopyUtilLibrary.tranConfig.GetType().GetProperties())
				{
					
					if(null!=prop.GetValue(TranCopyUtilLibrary.tranConfig) && null!= prop.Name){
						row = table.NewRow();
						row["parameter_name"]  = prop.Name;
						row["parameter_value"] = string.IsNullOrWhiteSpace(prop.GetValue(TranCopyUtilLibrary.tranConfig,  null).ToString())?"NULL":prop.GetValue(TranCopyUtilLibrary.tranConfig,  null).ToString();
						TranCopyUtilLibrary.writeToLog(prop.Name+": "+row["parameter_value"]);
						table.Rows.Add(row);
					}
						
				}
	
				reportTableMap.Add("Tran Copy Session Parameters",table);
                   	startTranCopy();

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
                public void startTranCopy(){
                   try{ 
                        
                        initConnectionStrings();
						if(!File.Exists(TranCopyUtilLibrary.copyScript)){

							 Console.WriteLine("Report source script "+TranCopyUtilLibrary.copyScript+" does not exist.");
							 TranCopyUtilLibrary.writeToLog("Report source script "+TranCopyUtilLibrary.copyScript+" does not exist.");
							 Environment.Exit(0);

						}
						if( serverIsReachable(connectionStringMap["source"])){
							if( string.IsNullOrEmpty(TranCopyUtilLibrary.destinationTable)){
								Console.WriteLine("No table has been specified for this Tran Copy session");
								TranCopyUtilLibrary.writeToLog("No table has been specified for this Tran Copy session");
                                Environment.Exit(0);
							}

							DateTime startParameterValue	   =  DateTime.Now;// DateTime.ParseExact(TranCopyUtilLibrary.copyStartParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                            DateTime endParameterValue	       =  DateTime.Now;// DateTime.ParseExact(TranCopyUtilLibrary.copyEndParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture); ;
                           
						     switch(TranCopyUtilLibrary.tranCopyMode){

								 case DAILY_COPY_MODE:
								 startParameterValue = string.IsNullOrEmpty(TranCopyUtilLibrary.copyStartParameterValue) || TranCopyUtilLibrary.copyStartParameterValue =="default" ?DateTime.Now:DateTime.ParseExact(TranCopyUtilLibrary.copyStartParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);

								int  numOfPrevDays          = TranCopyUtilLibrary.numOfDaysFromStart;
								endParameterValue           = startParameterValue.AddDays(-1*numOfPrevDays);

                            
							long startParamVal = long.Parse(startParameterValue.ToString("yyyyMMdd"));
							
							long endParamVal   = long.Parse(endParameterValue.ToString("yyyyMMdd"));

							long currentVal    = startParamVal;
                             numberOfTables = 0;
								while(currentVal   >= endParamVal){
									Console.WriteLine("currentVal: "+currentVal.ToString());
									copyDateMap.Add(numberOfTables, currentVal.ToString());
									TranCopyUtilLibrary.writeToLog("currentVal: "+currentVal.ToString());
								    partitionSizeIntervalMap.Add(currentVal.ToString(), currentVal.ToString());
									currentVal -=1; 
									++numberOfTables;								
								}
								 break;
								 case SPECIFIC_COPY_MODE:
								  startParameterValue	   =  DateTime.ParseExact(TranCopyUtilLibrary.copyStartParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                 endParameterValue	       = DateTime.ParseExact(TranCopyUtilLibrary.copyEndParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture); ;
                                  numberOfTables           =  0;
                                    
								   foreach(string  dateStr in  TranCopyUtilLibrary.tranCopySpecificParamterValues){
									    copyDateMap.Add(numberOfTables,DateTime.ParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture).ToString("yyyy-MM-dd"));
										partitionSizeIntervalMap.Add(DateTime.ParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture).ToString("yyyy-MM-dd"), DateTime.ParseExact(dateStr, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture).ToString("yyyy-MM-dd"));
                                        ++numberOfTables;
								   }

								 break;
								 case  RANGE_COPY_MODE:
								  startParameterValue	   =  DateTime.ParseExact(TranCopyUtilLibrary.copyStartParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
                                   endParameterValue	       = DateTime.ParseExact(TranCopyUtilLibrary.copyEndParameterValue.Replace("-","").Replace("/",""), "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture); ;
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
									
                  			    string copyScriptStr      =   File.ReadAllText(TranCopyUtilLibrary.copyScript);
							    string filterScriptStr    =  File.ReadAllText( TranCopyUtilLibrary.tranFilterCopyScript);

                    			getSchemaTable( copyScriptStr.Replace(TranCopyUtilLibrary.copyStartParameter,"\'2000-01-01\'"));
								getFilterTableSchema( filterScriptStr.Replace(TranCopyUtilLibrary.copyStartParameter,"2000-01-01")
								                                     .Replace("ARBITER_DESTINATION_TABLE",TranCopyUtilLibrary.destinationTable));

								if(schemaTable.Columns.Count > 0 ){

								tranCopyTables           		     = new System.Data.DataTable[numberOfTables];
								tranCopyThreads     		 			 = new Thread[numberOfTables];
								
							    tranCopyFilterTables      		     = new System.Data.DataTable[numberOfTables];
								tranCopyFilterThreads     			 = new Thread[numberOfTables];
								copyFilterScript						 = new string[numberOfTables];
								TranCopyUtilLibrary.concurrentThreads = numberOfTables;
								

                                int threadCounter = 0;
								foreach(KeyValuePair<string, string>  param in partitionSizeIntervalMap){
									
								 string  copySQL	         	= copyScriptStr.Replace(TranCopyUtilLibrary.copyStartParameter,param.Key);
								 copySQL    	      	        = copySQL.Replace(TranCopyUtilLibrary.copyEndParameter, param.Value);
                                
								 string  copyFilterSQL	        = filterScriptStr.Replace(TranCopyUtilLibrary.copyStartParameter,param.Key);
								 copyFilterSQL    	      	    = copyFilterSQL.Replace(TranCopyUtilLibrary.copyEndParameter, param.Value)
								                                            .Replace("ARBITER_DESTINATION_TABLE",TranCopyUtilLibrary.destinationTable);

								 
								
								  int  currentIndex				= threadCounter;
								
									  tranCopyThreads[currentIndex]  = 	new Thread(()=> {
										
										 runTranTransactionCopy("source",copySQL,currentIndex);
									
									});
									tranCopyThreads[currentIndex].Name    =   "tranCopyThread."+currentIndex.ToString();
									tranCopyThreadMap.Add(currentIndex,tranCopyThreads[currentIndex]);
									copyFilterScript[currentIndex]  		=	 copyFilterSQL;


									 tranCopyFilterThreads[currentIndex]  = 	new Thread(()=> {
										
										 runTranTransactionCopyFilter("destination",copyFilterSQL,currentIndex);
									
									});
									tranCopyFilterThreads[currentIndex].Name    =   "tranCopyThread."+currentIndex.ToString();
									tranCopyFilterThreadMap.Add(currentIndex,tranCopyFilterThreads[currentIndex]);
									++threadCounter;
                        			}
								
						        Console.WriteLine("Count: "+numberOfTables.ToString());
							    Console.WriteLine("Threads successfully initialized");
							    TranCopyUtilLibrary.writeToLog("Threads successfully initialized");
							    Console.WriteLine("WAIT_INTERVAL: "+TranCopyUtilLibrary.WAIT_INTERVAL.ToString());
							    TranCopyUtilLibrary.writeToLog("WAIT_INTERVAL: "+TranCopyUtilLibrary.WAIT_INTERVAL.ToString());
						        Console.WriteLine("TranCopyUtilLibrary.concurrentThreads: "+TranCopyUtilLibrary.concurrentThreads.ToString());
							
							//    foreach(Thread threadDetail in tranCopyThreads){
								for(int i=0; i< tranCopyThreads.Length; i++){


                                        Console.WriteLine("Starting thread: "+i.ToString());
										TranCopyUtilLibrary.writeToLog("Starting thread: "+i.ToString());
										if(tranCopyThreads[i]!= null) {
									      tranCopyThreads[i].Start();
										  runningThreadSet.Add(tranCopyThreads[i]);
									   }
									    if(tranCopyFilterThreads[i]!= null) {
									      tranCopyFilterThreads[i].Start();
										  runningThreadSet.Add(tranCopyFilterThreads[i]);
									   }
									
									 
							}
							TranCopyUtilLibrary.writeToLog("All threads started. Waiting for matching threads to finish");
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
								tranStagingTable                      = TranCopyUtilLibrary.tranCopyTableNamePrefix+"_"+TranCopyUtilLibrary.tranCopyType.ToLower()+"_"+i.ToString();
								filterTableName                       = TranCopyUtilLibrary.tranCopyFilterTablePrefix+"_"+TranCopyUtilLibrary.tranCopyType.ToLower()+"_"+i.ToString();

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
									row["transaction_copy_duration"]    		  = String.Format("{0} hour(s) {1} minute(s) {2} second(s)", tranCopyDuration.Hours, tranCopyDuration.Minutes, tranCopyDuration.Seconds);		
									TimeSpan tranFilterCopyDuration               = DateTime.Parse(copyFilterEndTimeMap[i], System.Globalization.CultureInfo.InvariantCulture).Subtract(DateTime.Parse(copyFilterStartTimeMap[i], System.Globalization.CultureInfo.InvariantCulture));
									row["filter_copy_duration"]  			      = String.Format("{0} hour(s) {1} minute(s) {2} second(s)", tranFilterCopyDuration.Hours, tranFilterCopyDuration.Minutes, tranFilterCopyDuration.Seconds);
									row["transaction_staging_count"]  			  = copyTransCountMap[i];
									row["filter_staging_count"]  			      = copyFilterTransCountMap[i];
									row["inserted_transaction_count"]  			  = copyTransCountMap[i] - copyFilterTransCountMap[i];
									table.Rows.Add(row);

								}
								
								reportTableMap.Add("Tran Session Details",table);



							 if(TranCopyUtilLibrary.sendEmailNotification)	sendMailNotification(reportTableMap);
						} else{
                    
					    Console.WriteLine("Could not get Schema for destination table");
                        TranCopyUtilLibrary.writeToLog("Could not get Schema for destination table");
                        Environment.Exit(0);

                    }



						}else{
								Console.WriteLine("Unable to connect to source database: "+TranCopyUtilLibrary.sourceServer);
								TranCopyUtilLibrary.writeToLog("Unable to connect to source database: "+TranCopyUtilLibrary.sourceServer);
								Environment.Exit(0);

						}

						
						
                   }catch(Exception e){

                         Console.WriteLine("Error: " + e.ToString());
						  Console.WriteLine("Error Message: " + e.Message);
                         Console.WriteLine(e.StackTrace);
						 TranCopyUtilLibrary.writeToLog("Error: " + e.ToString());
						 TranCopyUtilLibrary.writeToLog("Error Message: " + e.Message);
						 Console.WriteLine(e.ToString());
                   }
				    // TranCopyUtilLibrary.closeLogFile();
              }
           			  public static void initConnectionStrings(){
 				  
 				    sourceServerConnectionString    =  "Network Library=DBMSSOCN;Data Source=" +  TranCopyUtilLibrary.sourceConnectionProps.getSourceServer() + ","+TranCopyUtilLibrary.sourcePort+";database=" + TranCopyUtilLibrary.sourceConnectionProps.getSourceDatabase()+ ";User id=" + TranCopyUtilLibrary.sourceConnectionProps.getSourceUser()+ ";Password=" + TranCopyUtilLibrary.sourceConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;Packet Size=8192;";     
                     destServerConnectionString     =  "Network Library=DBMSSOCN;Data Source=" +  TranCopyUtilLibrary.destinationConnectionProps.getSourceServer() + ","+TranCopyUtilLibrary.destinationPort+";database=" + TranCopyUtilLibrary.destinationConnectionProps.getSourceDatabase()+ ";User id=" +  TranCopyUtilLibrary.destinationConnectionProps.getSourceUser()+ ";Password=" + TranCopyUtilLibrary.destinationConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;Packet Size=8192;";    
     				stagingConnectionString			=  "Network Library=DBMSSOCN;Data Source=" +  TranCopyUtilLibrary.stagingConnectionProps.getSourceServer() + ","+TranCopyUtilLibrary.stagingPort+";database=" + TranCopyUtilLibrary.stagingConnectionProps.getSourceDatabase()+ ";User id=" +  TranCopyUtilLibrary.stagingConnectionProps.getSourceUser()+ ";Password=" + TranCopyUtilLibrary.stagingConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;Packet Size=8192;";    
				    connectionStringMap.Add("source",sourceServerConnectionString);
 					connectionStringMap.Add("destination",destServerConnectionString);
					connectionStringMap.Add("staging",stagingConnectionString);
  
			  }

 				public   void  runTranTransactionCopy(string sourceServer,  string bulkQuery, int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString         = connectionStringMap[sourceServer];
			 	tranCopyTables[tabInd]               = new DataTable();
				string  tableName                       = TranCopyUtilLibrary.tranCopyTableNamePrefix+"_"+TranCopyUtilLibrary.tranCopyType.ToLower()+"_"+tabInd.ToString();
				copyTableNameMap.Add(tabInd,tableName);
				try{
                lock (locker)
                {	Console.WriteLine("Running fetch script for table: "+tabInd.ToString());

					if( TranCopyUtilLibrary.fs.BaseStream != null){
							TranCopyUtilLibrary.writeToLog("Running fetch script for table: " + tabInd.ToString()+"\n"+bulkQuery);
					}
				}
				copyStartTimeMap.Add(tabInd,DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                bulkCopyDataFromRemoteServer(bulkQuery, tableName);
				string  stagingTableIndex     		 = "IF((SELECT  1  FROM sys.indexes WHERE name='ix_"+TranCopyUtilLibrary.tranCopyFilterField+" ' AND object_id = OBJECT_ID('dbo.["+tableName+"]' )) = 1) BEGIN  DROP INDEX [ix_"+TranCopyUtilLibrary.tranCopyFilterField+"]  ON  dbo.["+tableName+"]   END ";
			     executeScript(stagingTableIndex, connectionStringMap["staging"]);               
				stagingTableIndex     		 = 		   " CREATE INDEX [ix_"+TranCopyUtilLibrary.tranCopyFilterField+"]  ON ["+tableName+"] (["+TranCopyUtilLibrary.tranCopyFilterField+"]);";
				executeScript(stagingTableIndex, connectionStringMap["staging"]);
				copyEndTimeMap.Add(tabInd,DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

			}catch(Exception e){

						
					
								Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+tranCopyTables[tabInd].ToString()+". Error: " + e.Message);
								Console.WriteLine(e.StackTrace);
								TranCopyUtilLibrary.writeToLog("Error while running bulk insert script "+bulkQuery+": " + e.Message);
								TranCopyUtilLibrary.writeToLog(e.ToString());
								TranCopyUtilLibrary.writeToLog(e.StackTrace);
								Console.WriteLine(e.ToString());
								emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert script "+bulkQuery+": " + e.Message);
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);
					
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
							 foreach(Thread arbCopyThread  in tranCopyThreads){
								
								if(arbCopyThread.IsAlive){
										++activeThreadCount;
                      				 
								}
						 	}

							 	 foreach(Thread arbCopyFilterThread  in tranCopyFilterThreads){
						
								if(arbCopyFilterThread.IsAlive){
										++activeThreadCount;
                      				 
								}
						 	}
            
                           if(completedThreadSet.Count !=tranCopyThreads.Length){
              
					        for (int i =  0;  i< tranCopyThreads.Length; i++ ) {
						
								if( !tranCopyThreads[i].IsAlive &&  !tranCopyFilterThreads[i].IsAlive){
									
									if(!completedThreadSet.Contains(i)){
											completedThreadSet.Add(i);
											
											/* Thread exportThread =  	new Thread(()=> {
										        exportDataToDestinationTable(i);
									
											});
											exportThread.Start();
											exportThread.Join();
											*/
									} 
								} else{
									++shouldWait;
								}
							}

							if(shouldWait>0){
									Console.WriteLine("Waiting for  " + TranCopyUtilLibrary.WAIT_INTERVAL.ToString());
								Thread.Sleep(TranCopyUtilLibrary.WAIT_INTERVAL);  
							}

							Console.WriteLine("Current completed thread count: "+completedThreadSet.Count.ToString());
							Console.WriteLine("Current running count: "+activeThreadCount.ToString());
							TranCopyUtilLibrary.writeToLog("Current completed thread count: " + completedThreadSet.Count.ToString());
							TranCopyUtilLibrary.writeToLog("Current running thread count: "+activeThreadCount.ToString());
						} else{


							for (int i =  0;  i< tranCopyThreads.Length; i++ ) {

								if( !tranCopyThreads[i].IsAlive &&  !tranCopyFilterThreads[i].IsAlive){
										if(!completedThreadSet.Contains(i)){

											completedThreadSet.Add(i);
											
										  /* 	Thread exportThread =  	new Thread(()=> {
										
													 exportDataToDestinationTable(i);
									
											});
											exportThread.Start();
											exportThread.Join();
											*/
											
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
               		    TranCopyUtilLibrary.writeToLog("Current completed thread count: " + completedThreadSet.Count.ToString());
                		TranCopyUtilLibrary.writeToLog("Current running thread count: " + activeThreadCount.ToString());
						if(skippedCount==tranCopyThreads.Length || shouldWait== 0){
							
							break;

						} 
				  }

				exportAllDataToDestinationTable();

               


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
								   TranCopyUtilLibrary.writeToLog("Error while connecting to server: " + e.Message);
								   TranCopyUtilLibrary.writeToLog(e.StackTrace);
								   TranCopyUtilLibrary.writeToLog(e.ToString());
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
                    TranCopyUtilLibrary.writeToLog("Executing script: "+theScript);
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
						 TranCopyUtilLibrary.writeToLog("Error while running fetch script: "+theScript+". The error is: "+e.Message);
						 TranCopyUtilLibrary.writeToLog("The data fetch session would now be restarted");
						 getDataFromSQL( theScript,  targetConnectionString );
						 TranCopyUtilLibrary.writeToLog(e.ToString());
					     emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+theScript+". The error is: "+e.Message);
						 emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
				
					} else {
						
						Console.WriteLine("Error while running script: " + e.Message);
						Console.WriteLine(e.StackTrace);
						 TranCopyUtilLibrary.writeToLog(e.ToString());
						 TranCopyUtilLibrary.writeToLog("Error while running script: " + e.Message);
						 TranCopyUtilLibrary.writeToLog(e.StackTrace);
						 Console.WriteLine(e.ToString());
						 TranCopyUtilLibrary.writeToLog(e.ToString());
						emailError.AppendLine("<div style=\"color:red\">Error while running script: " + e.Message);
						emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
						 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);
								
						 }
					}
					return  dt;
			  }
			  


             public System.Data.DataTable   getDataFromSourceDatabase (string script){
                            System.Data.DataTable dt = new DataTable();
                            try{

                                using (SqlConnection serverConnection =  new SqlConnection(connectionStringMap["source"])){
									SqlCommand cmd = new SqlCommand(script, serverConnection);
									TranCopyUtilLibrary.writeToLog("Executing script: "+script+" on source database.");
									cmd.CommandTimeout =0;
									serverConnection.Open();
									SqlDataReader  reader = cmd.ExecuteReader();
									dt.Load(reader);	
									cmd.Dispose();
                        }
                        }catch(Exception e){
								   TranCopyUtilLibrary.writeToLog("Error while running script: " + e.Message);
								   TranCopyUtilLibrary.writeToLog(e.StackTrace);
								   TranCopyUtilLibrary.writeToLog(e.ToString());
								   Console.WriteLine(e.ToString());
                        }
                        return dt;
                }

              				public static void  executeScript( string script, string  targetConnectionString){

						try{
							using (SqlConnection serverConnection =  new SqlConnection(targetConnectionString)){
									SqlCommand cmd = new SqlCommand(script, serverConnection);
									Console.WriteLine("Executing script: "+script);
									TranCopyUtilLibrary.writeToLog("Executing script: "+script);
									cmd.CommandTimeout =0;
									serverConnection.Open();
									cmd.ExecuteNonQuery();
							}
						}catch(Exception e){

									 Console.WriteLine("Error while running script: " + e.Message);
									 Console.WriteLine(e.StackTrace);
									 TranCopyUtilLibrary.writeToLog("Error while running script: " + e.Message);
									 TranCopyUtilLibrary.writeToLog(e.StackTrace);
									 TranCopyUtilLibrary.writeToLog(e.ToString());
									  Console.WriteLine(e.ToString());
									 emailError.AppendLine("<div style=\"color:red\">Error while running script: " + e.Message);
									 emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
									  if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);

							}
				}


public static void  restartableExecuteScript( string script, string  targetConnectionString){

						try{
							using (SqlConnection serverConnection =  new SqlConnection(targetConnectionString)){
									SqlCommand cmd = new SqlCommand(script, serverConnection);
									Console.WriteLine("Executing script: "+script);
									TranCopyUtilLibrary.writeToLog("Executing script: "+script);
									cmd.CommandTimeout =0;
									serverConnection.Open();
									cmd.ExecuteNonQuery();
							}
						}catch(Exception e){

									 Console.WriteLine("Error while running script: " + e.Message);
									 Console.WriteLine(e.StackTrace);
									 TranCopyUtilLibrary.writeToLog("Error while running script: " + e.Message);
									 TranCopyUtilLibrary.writeToLog(e.StackTrace);
									 TranCopyUtilLibrary.writeToLog(e.ToString());
									  Console.WriteLine(e.ToString());
									 if(!e.Message.ToLower().Contains("filegroup")){
									  restartableExecuteScript(script,targetConnectionString);
									 }
									 emailError.AppendLine("<div style=\"color:red\">Error while running script: " + e.Message);
									 emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
									  if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);

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
								bulkCopy.BatchSize =  TranCopyUtilLibrary.batchSize;
								bulkCopy.DestinationTableName = destTable;
								bulkCopy.WriteToServer(dTab);
                            }
                         }	
                    }catch(Exception e){
                       	
								Console.WriteLine("Error while running bulk insert: " + e.Message);
								Console.WriteLine(e.StackTrace);
								TranCopyUtilLibrary.writeToLog(e.ToString());
								 Console.WriteLine(e.ToString());
								TranCopyUtilLibrary.writeToLog("Error while running bulk insert: " + e.Message);
								TranCopyUtilLibrary.writeToLog(e.StackTrace);
								emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert: " + e.Message);
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);
						 
					}

                    
                }


	    public   static   void  runTranTransactionCopyFilter(string destinationServer,  string bulkQuery, int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString         = connectionStringMap[destinationServer];
			 	tranCopyFilterTables[tabInd]         = new DataTable();
				string  tableName                       = TranCopyUtilLibrary.tranCopyFilterTablePrefix+"_"+TranCopyUtilLibrary.tranCopyType.ToLower()+"_"+tabInd.ToString();
				 if(!copyFilterTableNameMap.ContainsKey(tabInd)){
					 copyFilterTableNameMap.Add(tabInd,tableName);
				 }
				try{
                lock (locker)
                {	Console.WriteLine("Running fetch script for table: "+tabInd.ToString());

					if( TranCopyUtilLibrary.fs.BaseStream != null){
							TranCopyUtilLibrary.writeToLog("Running fetch script for table: " + tabInd.ToString()+"\n"+bulkQuery);
					}
				}
				
					 if(!copyFilterStartTimeMap.ContainsKey(tabInd)){
						 copyFilterStartTimeMap.Add(tabInd,DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")); 
					 }         
                bulkCopyToStagingServer(bulkQuery, tableName, destServerConnectionString,stagingConnectionString, true, tabInd);
					 if(!copyFilterEndTimeMap.ContainsKey(tabInd)){
						copyFilterEndTimeMap.Add(tabInd,DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
					 }

					string  filterTableIndex    		 = "IF((SELECT  1  FROM sys.indexes WHERE name='ix_"+TranCopyUtilLibrary.tranCopyFilterField+" ' AND object_id = OBJECT_ID('dbo.["+tableName+"]' )) = 1) BEGIN  DROP INDEX [ix_"+TranCopyUtilLibrary.tranCopyFilterField+"]  ON  dbo.["+tableName+"]   END ";
				    executeScript(filterTableIndex,  connectionStringMap["staging"]);
				    filterTableIndex    		 		=				 " CREATE INDEX [ix_"+TranCopyUtilLibrary.tranCopyFilterField+"]  ON ["+tableName+"] (["+TranCopyUtilLibrary.tranCopyFilterField+"]); ";
					executeScript(filterTableIndex,  connectionStringMap["staging"]);
			

			 }catch(Exception e){
						Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+tranCopyFilterTables[tabInd].ToString()+". Error: " + e.Message);
						Console.WriteLine(e.StackTrace);
						TranCopyUtilLibrary.writeToLog("Error while running bulk insert script "+bulkQuery+": " + e.Message);
						TranCopyUtilLibrary.writeToLog(e.ToString());
						TranCopyUtilLibrary.writeToLog(e.StackTrace);
						Console.WriteLine(e.ToString());
						emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert script "+bulkQuery+": " + e.Message);
						emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
						 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);
			
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
                    TranCopyUtilLibrary.writeToLog("Executing script: " + copyScript);
                    cmd.CommandTimeout = 0;
                    if (serverConnection != null && serverConnection.State == ConnectionState.Closed)
                    {

                        serverConnection.Open();
                    }
                    SqlDataReader reader = cmd.ExecuteReader();
                    using (SqlConnection bulkCopyConnection = new SqlConnection(stagingConnectionString))
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.TableLock, null))
                        {
                            bulkCopyConnection.Open();
                            bulkCopy.BulkCopyTimeout	  = 0;
                            bulkCopy.BatchSize 			  = TranCopyUtilLibrary.batchSize;
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
								TranCopyUtilLibrary.writeToLog("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								TranCopyUtilLibrary.writeToLog("The data fetch session would now be restarted");
								TranCopyUtilLibrary.writeToLog(e.ToString());
								Console.WriteLine(e.ToString());
								bulkCopyDataFromRemoteServer( copyScript,  destTable); 
								emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {

							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							TranCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							TranCopyUtilLibrary.writeToLog(e.StackTrace);
							 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);

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
                    TranCopyUtilLibrary.writeToLog("Executing script: " + copyScript);
                    cmd.CommandTimeout = 0;
                    if (serverConnection != null && serverConnection.State == ConnectionState.Closed)
                    {

                        serverConnection.Open();
                    }
                    SqlDataReader reader = cmd.ExecuteReader();
                    using (SqlConnection bulkCopyConnection = new SqlConnection(stageConStr))
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.TableLock , null))
                        {
                            bulkCopyConnection.Open();
                            bulkCopy.BulkCopyTimeout	  = 0;
                            bulkCopy.BatchSize 			  = TranCopyUtilLibrary.batchSize;
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
								TranCopyUtilLibrary.writeToLog("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								TranCopyUtilLibrary.writeToLog("The data fetch session would now be restarted");
								TranCopyUtilLibrary.writeToLog(e.ToString());
								Console.WriteLine(e.ToString());
								 runTranTransactionCopyFilter("destination",copyFilterScript[tableIndex],tableIndex);
								 string  filterTableIndex    		     =  "IF((SELECT  1  FROM sys.indexes WHERE name='ix_"+TranCopyUtilLibrary.tranCopyFilterField+" ' AND object_id = OBJECT_ID('dbo.["+copyFilterTableNameMap[tableIndex]+"]' )) = 1) BEGIN  DROP INDEX [ix_"+TranCopyUtilLibrary.tranCopyFilterField+"]  ON  dbo.["+copyFilterTableNameMap[tableIndex]+"]  END";
							    executeScript(filterTableIndex, connectionStringMap["staging"]);					   
								filterTableIndex=	" CREATE INDEX [ix_"+TranCopyUtilLibrary.tranCopyFilterField+"]  ON ["+copyFilterTableNameMap[tableIndex]+"] (["+TranCopyUtilLibrary.tranCopyFilterField+"]);";
								executeScript(filterTableIndex, connectionStringMap["staging"]);
								bulkCopyToStagingServer( copyScript, destTable,connectionString,stageConStr,createTable,tableIndex); 
								emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {
							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							TranCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							TranCopyUtilLibrary.writeToLog(e.StackTrace);
							 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);

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
                TranCopyUtilLibrary.writeToLog("Error getting  table schema from server with script:\n" + sqlScript + ". \nError " + e.Message);
                TranCopyUtilLibrary.writeToLog(e.StackTrace);
             


            }

            return schemaTable;
        }
     public static  void  exportDataToDestinationTable( int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString      = connectionStringMap["destination"];
				string   stagingTableName            = TranCopyUtilLibrary.tranCopyTableNamePrefix+"_"+TranCopyUtilLibrary.tranCopyType.ToLower()+"_"+tabInd.ToString();
				string   filterTableName             = TranCopyUtilLibrary.tranCopyFilterTablePrefix+"_"+TranCopyUtilLibrary.tranCopyType.ToLower()+"_"+tabInd.ToString();
				

				string  bulkInsertScript             = "SELECT  * FROM  ["+stagingTableName+"] WITH (NOLOCK)  WHERE  ["+TranCopyUtilLibrary.tranCopyFilterField+"] NOT IN (SELECT ["+TranCopyUtilLibrary.tranCopyFilterField+"]  FROM  ["+filterTableName+"]   WITH (NOLOCK))";
			  try{
				  
                bulkCopyDataToDestinationServer(bulkInsertScript, TranCopyUtilLibrary.destinationTable, connectionStringMap["destination"],tabInd);
                
			}catch(Exception e){

								Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+tranCopyTables[tabInd].ToString()+". Error: " + e.Message);
								Console.WriteLine(e.StackTrace);
								TranCopyUtilLibrary.writeToLog(e.ToString());
								TranCopyUtilLibrary.writeToLog(e.StackTrace);
								Console.WriteLine(e.ToString());
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);
					
						}
				}
				    public static void bulkCopyDataToDestinationServer( string copyScript, string destTable, string connectionStr)
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
					if(columns.Count==0){

						 columns =  TranCopyUtilLibrary.destinationTableColumnOrder;
					}

                    SqlCommand cmd = new SqlCommand(copyScript , serverConnection);

                    cmd = new SqlCommand(copyScript, serverConnection);
                    Console.WriteLine("Executing script: " + copyScript);
                    TranCopyUtilLibrary.writeToLog("Executing script: " + copyScript);
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
                            bulkCopy.BatchSize 			  = TranCopyUtilLibrary.batchSize;

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



							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							TranCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							TranCopyUtilLibrary.writeToLog(e.StackTrace);
							emailError.AppendLine("<div style=\"color:red\">"+e.Message);
							emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
							 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);

					
               
            }
        }
		        public static void bulkCopyDataToDestinationServer( string copyScript, string destTable, string connectionStr, int tableIndex)
        {

                         
           try
            {
                using (SqlConnection serverConnection = new SqlConnection(stagingConnectionString))
                {
                    
					DataTable schmTable = getColumns(copyScript, stagingConnectionString);
					ArrayList columns   = new ArrayList();

					foreach (DataColumn col in schmTable.Columns){
				     columns.Add(col.ColumnName);
					}
					if(columns.Count==0){

						 columns =  TranCopyUtilLibrary.destinationTableColumnOrder;
					}

                    SqlCommand cmd = new SqlCommand(copyScript , serverConnection);

                    cmd = new SqlCommand(copyScript, serverConnection);
                    Console.WriteLine("Executing script: " + copyScript);
                    TranCopyUtilLibrary.writeToLog("Executing script: " + copyScript);
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
                            bulkCopy.BatchSize 			  = TranCopyUtilLibrary.batchSize;

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
								TranCopyUtilLibrary.writeToLog("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								TranCopyUtilLibrary.writeToLog("The data fetch session would now be restarted");
								TranCopyUtilLibrary.writeToLog(e.ToString());
								Console.WriteLine(e.ToString());
                                runTranTransactionCopyFilter("destination",copyFilterScript[tableIndex],tableIndex);
								 string  filterTableIndex    		     =  "IF((SELECT  1  FROM sys.indexes WHERE name='ix_"+TranCopyUtilLibrary.tranCopyFilterField+" ' AND object_id = OBJECT_ID('dbo.["+copyFilterTableNameMap[tableIndex]+"]' )) = 1) BEGIN  DROP INDEX [ix_"+TranCopyUtilLibrary.tranCopyFilterField+"]  ON  dbo.["+copyFilterTableNameMap[tableIndex]+"]  END";
						        executeScript(filterTableIndex, connectionStringMap["staging"]);               	  
								filterTableIndex    		     =   " CREATE INDEX [ix_"+TranCopyUtilLibrary.tranCopyFilterField+"]  ON ["+copyFilterTableNameMap[tableIndex]+"] (["+TranCopyUtilLibrary.tranCopyFilterField+"]);";
								executeScript(filterTableIndex, connectionStringMap["staging"]);
								bulkCopyDataToDestinationServer( copyScript,  destTable,connectionStr, tableIndex); 
								emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {

							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							TranCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							TranCopyUtilLibrary.writeToLog(e.StackTrace);
							 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);

					}
               
            }
        }

public static  void  exportAllDataToDestinationTable( ){

				string   targetConnectionString      = connectionStringMap["destination"];
				StringBuilder finalInsert         =  new StringBuilder();
				string  tranStagingTablePrefix    = TranCopyUtilLibrary.tranCopyTableNamePrefix+"_"+TranCopyUtilLibrary.tranCopyType.ToLower(); 
				string  filterTableNamePrefix     = TranCopyUtilLibrary.tranCopyFilterTablePrefix+"_"+TranCopyUtilLibrary.tranCopyType.ToLower(); 
				string  insertScriptTemplate      = "SELECT  "+TranCopyUtilLibrary.finalSelectFields +"  FROM  [TRANSACTION_TABLE_PLACEHOLDER] WITH (NOLOCK)  WHERE  [tran_nr] NOT IN (SELECT [tran_nr]  FROM  [FILTER_TABLE_PLACEHOLDER]   WITH (NOLOCK))";

				finalInsert.AppendLine( " SELECT  * FROM  ( ");

				for(int  i=0; i< numberOfTables; i++){
						finalInsert.AppendLine(insertScriptTemplate.Replace("TRANSACTION_TABLE_PLACEHOLDER", tranStagingTablePrefix+"_"+i.ToString()).Replace("FILTER_TABLE_PLACEHOLDER", filterTableNamePrefix+"_"+i.ToString()));
						 finalInsert.AppendLine("  UNION ALL ");
				}
				for(int  j=0; j<12;  j++)finalInsert.Length--;

				finalInsert.AppendLine(" ) insert_table");
				

			  try{
				  
               		// bulkCopyDataToDestinationServer(finalInsert.ToString(), TranCopyUtilLibrary.destinationTable, connectionStringMap["destination"]);
					 string  finalStagingtable   =TranCopyUtilLibrary.filteredStagingTablePrefix+"_"+TranCopyUtilLibrary.tranCopyType+"_999";
					 bulkCopyToStagingTable(finalInsert.ToString(),finalStagingtable, connectionStringMap["staging"],connectionStringMap["destination"],true );
				     string  mergeSql      = TranCopyUtilLibrary.tableMergeScript.Replace("TABLE_INSERT_LIST",TranCopyUtilLibrary.finalSelectFields)
					 															     .Replace("TEE_PREFIX_TABLE_INSERT_LIST","t."+TranCopyUtilLibrary.finalSelectFields.Replace(",",",t."))
																					   .Replace("SOURCE_TABLE",finalStagingtable)
																					    .Replace("TEE_PREFIX_TABLE_INSERT_LIST",TranCopyUtilLibrary.destinationTable);
				  	    String  createIndexScript  = "CREATE INDEX ix_"+TranCopyUtilLibrary.tranCopyFilterField+" ON ["+TranCopyUtilLibrary.filteredStagingTablePrefix+"_"+TranCopyUtilLibrary.tranCopyType+"_999] (["+TranCopyUtilLibrary.tranCopyFilterField+"] );";
						executeScript(createIndexScript,connectionStringMap["destination"]);
					    restartableExecuteScript(mergeSql,connectionStringMap["destination"]);
			    }catch(Exception e){

								//Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+tranCopyTables[tabInd].ToString()+". Error: " + e.Message);
								Console.WriteLine(e.StackTrace);
								TranCopyUtilLibrary.writeToLog(e.ToString());
								TranCopyUtilLibrary.writeToLog(e.StackTrace);
								Console.WriteLine(e.ToString());
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);
					
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
                TranCopyUtilLibrary.writeToLog("Error getting  table schema from server with script:\n" + sqlScript + ". \nError " + e.Message);
                TranCopyUtilLibrary.writeToLog(e.StackTrace);
             
            }


        } 

        public static void getFilterTableSchema(string sqlScript)
        {
           try{
            sqlScript =sqlScript.Contains("#")? sqlScript:"SET FMTONLY ON \n" + sqlScript.Replace(", INDEX=ix_"+TranCopyUtilLibrary.tranCopyFilterField,"");
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
                TranCopyUtilLibrary.writeToLog("Error getting filter table schema from server with script:\n" + sqlScript + ". \nError " + e.Message);
                TranCopyUtilLibrary.writeToLog(e.StackTrace);
             
            }


        }

public static void bulkCopyToStagingTable( string copyScript, string destTable, string sourceConString,string destConStr,  bool createTable)
        {
             DataTable schmTable   =getColumns(copyScript,sourceConString);
            if(createTable) createSQLTableFromDataTable(destTable, schmTable, destConStr);
           try
            {
                using (SqlConnection serverConnection = new SqlConnection(sourceConString))
                {
                   
                    SqlCommand cmd = new SqlCommand(copyScript , serverConnection);

                    cmd = new SqlCommand(copyScript, serverConnection);
                    Console.WriteLine("Executing script: " + copyScript);
                    TranCopyUtilLibrary.writeToLog("Executing script: " + copyScript);
                    cmd.CommandTimeout = 0;
                    if (serverConnection != null && serverConnection.State == ConnectionState.Closed)
                    {

                        serverConnection.Open();
                    }
                    SqlDataReader reader = cmd.ExecuteReader();
                    using (SqlConnection bulkCopyConnection = new SqlConnection(destConStr))
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.TableLock , null))
                        {
                            bulkCopyConnection.Open();
                            bulkCopy.BulkCopyTimeout	  = 0;
                            bulkCopy.BatchSize 			  = TranCopyUtilLibrary.batchSize;
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
								Console.WriteLine("Error copying data to staging table with script: "+copyScript+". The error is: "+e.Message);
								Console.WriteLine("The staging session would now be restarted");
								TranCopyUtilLibrary.writeToLog("Error copying data to staging table with script: "+copyScript+". The error is: "+e.Message);
								TranCopyUtilLibrary.writeToLog("The staging session would now be restarted");
								TranCopyUtilLibrary.writeToLog(e.ToString());
								Console.WriteLine(e.ToString());
								bulkCopyToStagingTable(copyScript,  destTable,  sourceConString, destConStr ,   createTable);
								emailError.AppendLine("<div style=\"color:red\">Error staging script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {
							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							TranCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							TranCopyUtilLibrary.writeToLog(e.StackTrace);
							 if(TranCopyUtilLibrary.sendMailOnError)	sendMailNotification(reportTableMap);

					}
               
            }
        }

public  static void sendMailNotification(Dictionary<string, DataTable> dTableMap){

	            Console.WriteLine("Sending Notification... ");
				TranCopyUtilLibrary.writeToLog("Sending Notification... ");
				emailBody.AppendLine("<div style=\"color:black\">Hi, All.</div>");
				emailBody.AppendLine("<div style=\"color:black\">\n</div>");
				emailBody.AppendLine("<div style=\"color:black\">Trust this meets you well</div>");
				emailBody.AppendLine("<div style=\"color:black\">\n</div>");
				emailBody.AppendLine("<div style=\"color:black\">Please see report for "+TranCopyUtilLibrary.tranCopyType+" Copy Session below: </div>");
                MailMessage message = new MailMessage();

				foreach (var address in TranCopyUtilLibrary.toAddress.Split(new [] {TranCopyUtilLibrary.emailSeparator}, StringSplitOptions.RemoveEmptyEntries)){
						if(!string.IsNullOrWhiteSpace(address)){
									message.To.Add(address);   	
						}
				}
				foreach (var address in TranCopyUtilLibrary.ccAddress.Split(new [] {TranCopyUtilLibrary.emailSeparator}, StringSplitOptions.RemoveEmptyEntries)){
					if(!string.IsNullOrWhiteSpace(address)){
					message.CC.Add(address);   	
					}
				}
				foreach (var address in TranCopyUtilLibrary.bccAddress.Split(new [] {TranCopyUtilLibrary.emailSeparator}, StringSplitOptions.RemoveEmptyEntries)){
							if(!string.IsNullOrWhiteSpace(address)){
										message.Bcc.Add(address);   	
							}
				}
				Console.WriteLine("Sending Notification... ");
				message.From = new MailAddress(TranCopyUtilLibrary.fromAddress);				
				message.Subject = "Tran Transaction Copy Report for "+TranCopyUtilLibrary.tranCopyType+" at  "+DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");
				message.IsBodyHtml = true;
		
				emailBody.AppendLine("<style type=\"text/css\">");
				emailBody.AppendLine("table.gridtable {");
				emailBody.AppendLine("	font-family:"+TranCopyUtilLibrary.emailFontFamily+";");
				emailBody.AppendLine("	font-size:"+TranCopyUtilLibrary.emailFontSize+";");
				emailBody.AppendLine("	color:"+TranCopyUtilLibrary.borderColour+";");
				emailBody.AppendLine("	border-width:"+TranCopyUtilLibrary.borderWidth+";");
				emailBody.AppendLine("	border-color: "+TranCopyUtilLibrary.borderColour+";");
				emailBody.AppendLine("	border-collapse: collapse;");
				emailBody.AppendLine("}");
				emailBody.AppendLine("table.gridtable th {");
				emailBody.AppendLine("	border-width: "+TranCopyUtilLibrary.borderWidth+";");
				emailBody.AppendLine("	padding: 8px;");
				emailBody.AppendLine("	border-style: solid;");
				emailBody.AppendLine("	border-color:"+TranCopyUtilLibrary.borderColour+";");
				emailBody.AppendLine("	background-color:"+TranCopyUtilLibrary.headerBgColor+";");
				emailBody.AppendLine("}");
				emailBody.AppendLine("table.gridtable td {");
				emailBody.AppendLine("	border-width: 1px;");
				emailBody.AppendLine("	padding: 8px;");
				emailBody.AppendLine("	border-style: solid;");
				emailBody.AppendLine("	border-color: "+TranCopyUtilLibrary.borderColour+";");
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
								emailBody.AppendLine("<tr style=\"background-color:"+TranCopyUtilLibrary.alternateRowColour+"\">"); //<td>"+row["INDEX_NO"]+"</td><td>"+row["PARAMETER"]+"</td><td>"+row["VALUE"]+"</td></tr>");
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

			smtpClient.Host = TranCopyUtilLibrary.smtpServer;
			smtpClient.Port = Int32.Parse(TranCopyUtilLibrary.smtpPort.ToString());
			smtpClient.EnableSsl = TranCopyUtilLibrary.isSSLEnabled;
			smtpClient.Credentials = new System.Net.NetworkCredential(TranCopyUtilLibrary.sender, TranCopyUtilLibrary.senderPassword);
			smtpClient.Send(message);

		}

			public static void cleanUp(){

                    string  filterTableDeleteScript   =""; 
					string  stagingTableDeleteScript  = "";
                    for(int i=0; i<numberOfTables; i++){
					   stagingTableDeleteScript            = " IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].["+copyTableNameMap[i]+"]') AND type in (N'U')) BEGIN  DROP TABLE ["+copyTableNameMap[i]+"] END";
					   filterTableDeleteScript             = " IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].["+copyFilterTableNameMap[i]+"]') AND type in (N'U')) BEGIN  DROP TABLE ["+copyFilterTableNameMap[i]+"] END";
					   Console.WriteLine("Removing filter table: "+copyFilterTableNameMap[i]+"...");
					   TranCopyUtilLibrary.writeToLog("Removing filter table: "+copyFilterTableNameMap[i]+"...");
					   executeScript(filterTableDeleteScript, connectionStringMap["staging"]);				
					   Console.WriteLine("Removing staging table: "+copyTableNameMap[i]+"...");
					   TranCopyUtilLibrary.writeToLog("Removing staging table: "+copyTableNameMap[i]+"...");
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
						 
						 new  TranCopy();
						 
					 }else {					
							new  TranCopy(configFile);
					 }
					
					}catch(Exception e) {
					   
					   Console.WriteLine(e.Message);

					   Console.WriteLine(e.StackTrace);
                	 
					
					}
				

                }

            }

}