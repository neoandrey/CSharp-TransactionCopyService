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

				 internal  static  const int						DAILY_COPY_MODE						= 0;

				 internal static   const int                        SPECIFIC_COPY_MODE                  =  1;

                internal   static  const  int                       RANGE_COPY_MODE						= 2;   

				internal          

                  // internal  static int   					ThreadCount                          =  0;

                 static readonly object locker = new object();

				// static bool   runWebServer                                 =     false;

				 public ArbiterCopy(){

						new  ArbiterCopyUtilLibrary();
						startArbiterCopy();
            	
				 }

                public ArbiterCopy(string  config){
				  if(File.Exists(nuConfig)){
				
					string  nuConfig   = config.Contains("\\\\")? config:config.Replace("\\", "\\\\");
					new  ArbiterCopyUtilLibrary(config);
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

							DateTime startParameterValue	 = DateTime.ParseExact(ArbiterCopyUtilLibrary.copyStartParameterValue, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture); ;
                            DateTime endParameterValue	 = DateTime.ParseExact(ArbiterCopyUtilLibrary.copyStartParameterValue, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture); ;
                           
						     switch(ArbiterCopyUtilLibrary.arbiterCopyMode){

								 case DAILY_COPY_MODE:

								int  numOfPrevDays          = ArbiterCopyUtilLibrary.numOfDaysFromStart;
								endParameterValue           = startParameterValue.AddDays(-1*numOfPrevDays);


							long startParamVal = long.Parse(startParameterValue.ToString("yyyy-MM-dd"));
							
							long endParamVal   = long.Parse(endParameterValue.ToString("yyyy-MM-dd"));

							long currentVal    = endParamVal;

								while(currentVal  <= startParamVal){

								    partitionSizeIntervalMap.Add(currentVal.ToString(), currentVal.ToString());
									currentVal +=1; 
									++numberOfTables;								
								}
								 break;
								 case SPECIFIC_COPY_MODE:

								   foreach(string  dateStr in  ArbiterCopyUtilLibrary.arbiterCopySpecificParamterValues){
		
										partitionSizeIntervalMap.Add(DateTime.ParseExact(dateStr, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture).ToString("yyyy-MM-dd"), DateTime.ParseExact(dateStr, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture).ToString("yyyy-MM-dd"));

								   }

								 break;
								 case  RANGE_COPY_MODE:

									    startParamVal = long.Parse(startParameterValue.ToString( "yyyy-MM-dd"));
										currentVal    = startParamVal;
									    endParamVal   = long.Parse(endParameterValue.ToString( "yyyy-MM-dd"));
										while( currentVal <= endParamVal){

												partitionSizeIntervalMap.Add(startParamVal.ToString(), endParamVal.ToString());
												++numberOfTables;
												currentVal   = currentVal  +1;

										}
								 break;
							 }

						   

							// hasQuotes(ArbiterCopyUtilLibrary.partitioningParameterMinVal)?ArbiterCopyUtilLibrary.partitioningParameterMinVal:quoteString(ArbiterCopyUtilLibrary.partitioningParameterMinVal) ;
				

									
                  			    string copyScriptStr      =   File.ReadAllText(ArbiterCopyUtilLibrary.copyScript);
							    string filterScriptStr    =  File.ReadAllText( ArbiterCopyUtilLibrary.arbiterFilterCopyScript);

                    			getSchemaTable( copyScriptStr.Replace(ArbiterCopyUtilLibrary.copyStartParameter,"\'2000-01-01\'"));
								getFilterTableSchema( filterScriptStr.Replace(ArbiterCopyUtilLibrary.copyStartParameter,"\'2000-01-01\'")
								                                     .Replace("ARBITER_DESTINATION_TABLE",ArbiterCopyUtilLibrary.destinationTable));

								if(schemaTable.Columns.Count > 0 ){

								arbiterCopyTables           		     = new System.Data.DataTable[numberOfTables];
								arbiterCopyThreads     		 			 = new Thread[numberOfTables];
								
							    arbiterCopyFilterTables      		     = new System.Data.DataTable[numberOfTables];
								arbiterCopyFilterThreads     			 = new Thread[numberOfTables];

								ArbiterCopyUtilLibrary.concurrentThreads = numberOfTables;
								

                                int threadCounter = 0;
								foreach(KeyValuePair<string, string>  param in partitionSizeIntervalMap){
									
								 string  copySQL	         	= copyScriptStr.Replace(ArbiterCopyUtilLibrary.copy_start_parameter,param.Key);
								 copySQL    	      	        = copySQL.Replace(ArbiterCopyUtilLibrary.copy_end_parameter, param.Value);
                                
								 string  copyFilterSQL	        = filterScriptStr.Replace(ArbiterCopyUtilLibrary.copy_start_parameter,param.Key);
								 copyFilterSQL    	      	    = copyFilterSQL.Replace(ArbiterCopyUtilLibrary.copy_end_parameter, param.Value)
								                                            .Replace("ARBITER_DESTINATION_TABLE",ArbiterCopyUtilLibrary.destinationTable);

								 
								
								  int  currentIndex				= threadCounter;
								
									  arbiterCopyThreads[currentIndex]  = 	new Thread(()=> {
										
										 runArbiterTransactionCopy("source",copySQL,currentIndex);
									
									});
									arbiterCopyThreads[currentIndex].Name    =   "arbiterCopyThread."+currentIndex.ToString();
									arbiterCopyThreadMap.Add(currentIndex,arbiterCopyThreads[currentIndex]);


									 arbiterCopyFilterThreads[currentIndex]  = 	new Thread(()=> {
										
										 runArbiterTransactionCopyFilter("target",copyFilterSQL,currentIndex);
									
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
							    int trCounter  = 0;
							    foreach(Thread threadDetail in arbiterCopyThreads){
                                    Console.WriteLine("Checking for the number of running threads...");
                                    wait();
									if(runningThreadSet.Count < (ArbiterCopyUtilLibrary.concurrentThreads*2)) {

                                        Console.WriteLine("Current number of threads running: "+runningThreadSet.Count.ToString());
										Console.WriteLine("Waiting for  "+ArbiterCopyUtilLibrary.WAIT_INTERVAL.ToString());
									   if(threadDetail!= null) {
									      threadDetail.Start();
										  runningThreadSet.Add(threadDetail);
									   }
									    if(arbiterCopyFilterThreads[trCounter]!= null) {
									      arbiterCopyFilterThreads[trCounter].Start();
										  runningThreadSet.Add(arbiterCopyFilterThreads[trCounter]);
									   }
									   ++trCounter;
									}
							}
							 
							
                               while(runningThreadSet.Count!=0){
                       				 wait();
							   }
                            

							//  createFinalView(numberOfTables);


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
     				connectionStringMap.Add("source",sourceServerConnectionString);
 					connectionStringMap.Add("destination",destServerConnectionString);
  
			  }

 				public   void  runArbiterTransactionCopy(string sourceServer,  string bulkQuery, int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString         = connectionStringMap[sourceServer];
			 	arbiterCopyTables[tabInd]               = new DataTable();
				string  tableName                       = ArbiterCopyUtilLibrary.arbiterCopyTableNamePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType+"_"+tabInd.ToString();
				try{
                lock (locker)
                {	Console.WriteLine("Running fetch script for table: "+tabInd.ToString());

					if( ArbiterCopyUtilLibrary.fs.BaseStream != null){
							ArbiterCopyUtilLibrary.writeToLog("Running fetch script for table: " + tabInd.ToString()+"\n"+bulkQuery);
					}
				}

                bulkCopyDataFromRemoteServer(bulkQuery, tableName);

			}catch(Exception e){

						
					
								Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+arbiterCopyTables[tabInd].ToString()+". Error: " + e.Message);
								Console.WriteLine(e.StackTrace);
								ArbiterCopyUtilLibrary.writeToLog("Error while running bulk insert script "+bulkQuery+": " + e.Message);
								ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
								Console.WriteLine(e.ToString());
								emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert script "+bulkQuery+": " + e.Message);
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
					
						}
				}
			
			  public static void wait(){

				  int activeThreadCount               =  0;
				  HashSet<int> completedThreadSet     =  new  HashSet<int>();
				  int  threadCount                    =  0;
				  int shouldWait                      =  1;
				  int  skippedCount					  =  0;
				  while(shouldWait!= 0){
					  		 shouldWait               =  0;
						     skippedCount			  =  0;
                             
							 foreach(Thread arbCopyThread  in arbiterCopyThreads){
								
								if(arbCopyThread.IsAlive){
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
								activeThreadCount = 0;
							}

							Console.WriteLine("Current completed thread count: "+completedThreadSet.Count.ToString());
							Console.WriteLine("Current running count: "+runningThreadSet.Count.ToString());
							ArbiterCopyUtilLibrary.writeToLog("Current completed thread count: " + completedThreadSet.Count.ToString());
							ArbiterCopyUtilLibrary.writeToLog("Current running thread count: "+runningThreadSet.Count.ToString());
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
                		Console.WriteLine("Current running count: " + runningThreadSet.Count.ToString());
               		    ArbiterCopyUtilLibrary.writeToLog("Current completed thread count: " + completedThreadSet.Count.ToString());
                		ArbiterCopyUtilLibrary.writeToLog("Current running thread count: " + runningThreadSet.Count.ToString());
						if(skippedCount==arbiterCopyThreads.Length)break;
				  }



               


            }
			  

			  public static void outputTableData(int  tableIndex){
                // string     tableFileName  =   getTempFilePath(tableIndex) + "\\" + ArbiterCopyUtilLibrary.temporaryFileNamePrefix + "_" + tableIndex.ToString() + ".csv";
                // Console.WriteLine("Reading file: " +tableFileName);
                 // ArbiterCopyUtilLibrary.writeToLog("Reading file: " + tableFileName);
			     //  DataTable reportTable     =   readStoredTableData(tableFileName);
			     // loadDataFromSQLite(tableIndex); 
				  lock (locker)
                	{
			     if(ArbiterCopyUtilLibrary.reportOutputMethod == ArbiterCopyUtilLibrary.TABLE_OUTPUT_METHOD){
						string server  = "destination";
						if(isFirstInsert){

								createSQLTableFromDataTable(ArbiterCopyUtilLibrary.reportTableName, arbiterCopyTables[tableIndex], connectionStringMap[server]);	
								isFirstInsert = false;				
						}
                 
                			bulkCopyTableData(server,arbiterCopyTables[tableIndex],ArbiterCopyUtilLibrary.reportTableName);
				 

            /*    if (File.Exists(tableFileName))
                {
                    File.Delete(tableFileName);
                }

				*/
			 }
					}

				arbiterCopyTables[tableIndex].Clear();
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
			  
              public   DataTable  getDataFromSQL(string theScript, string targetConnectionString ){
	         
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

							}
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
						 
					}

                    
                }


      public  static  void  exportTableToCSV(DataTable dt, string  fileName){

					StringBuilder sb = new StringBuilder(); 

					if(File.Exists(fileName))
						{
							File.Delete(fileName);
						}

					IEnumerable<string> columnNames = dt.Columns.Cast<DataColumn>().
					Select(column => column.ColumnName);
					sb.AppendLine(string.Join(",", columnNames));

					foreach (DataRow row in dt.Rows)
					{
					IEnumerable<string> fields = row.ItemArray.Select(field => field.ToString());
					sb.AppendLine(string.Join(",", fields));
					}

					File.WriteAllText(fileName, sb.ToString());

	  }

        public static DataTable readStoredTableData(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                string[] headers = sr.ReadLine().Split(new string[] { ArbiterCopyUtilLibrary.temporaryFileFieldDelimeter }, StringSplitOptions.None);
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(new string[] { ArbiterCopyUtilLibrary.temporaryFileFieldDelimeter }, StringSplitOptions.None);
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            return dt;
        }

        public static string getTempFilePath(int fileIndex)
        {

            int mapSize = ArbiterCopyUtilLibrary.tempFileDriveMap.Count;
            int modulo = fileIndex % mapSize;
            return ArbiterCopyUtilLibrary.tempFilePathMap[modulo];

        }

		public  void  exportToSQLite(int  index){
		
		string dbFileName       =  getTempFilePath(index)+ "\\" + ArbiterCopyUtilLibrary.temporaryFileNamePrefix + "_" + index.ToString() + ".sqlite";
		string sqliteTableName  =  ArbiterCopyUtilLibrary.temporaryTableName+"_"+index.ToString();
		if (!File.Exists(dbFileName)){
		 try{
            using( SQLiteConnection liteConnect = new SQLiteConnection("Data Source="+dbFileName+";Version=3;")){
			liteConnect.Open();
			string sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='"+sqliteTableName+"';";
			Console.WriteLine("Running: "+sql);

			SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
			Object result = command.ExecuteScalar();
			command.Dispose();
			Console.WriteLine("result: "+result.ToString());
			
			if(result.ToString() != "1"){
				
			  StringBuilder  createSqlBuilder   = new StringBuilder();
			  createSqlBuilder.Append(" CREATE TABLE ["+sqliteTableName+"] ( ");
			  string colDataType  = "";
			  foreach(DataColumn col in arbiterCopyTables[index].Columns){
		 
		 		if(col.DataType.Name.ToString()=="Boolean")
		 		{
		 		colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="Byte")
		 		{
		 		colDataType = "BLOB";
		 		}
		 		if(col.DataType.Name.ToString()=="Char")
		 		{
		 		    colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="TEXT")
		 		{
		 		colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="Decimal")
		 		{
		 		colDataType = "REAL";
		 		}
		 		if(col.DataType.Name.ToString()=="Double")
		 		{
		 		colDataType = "REAL";
		 		}
		 		if(col.DataType.Name.ToString()=="Int16")
		 		{
		 		colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="Int32")
		 		{
		 		colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="Int64")
		 		{
		 		colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="SByte")
		 		{
		 		colDataType = "BLOB";
		 		}
		 		if(col.DataType.Name.ToString()=="Single")
		 		{
		 		colDataType = "BLOB";
		 		}
		 		if(col.DataType.Name.ToString()=="String")
		 
		 		{
		 			colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="TimeSpan")
		 
		 		{
		 			colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="UInt16")
		 
		 		{
		 			colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="UInt32")
		 
		 		{
		 			colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="UInt64")
		 
		 		{
		 			colDataType = "INTEGER";
		 		}
		 		
					createSqlBuilder.Append(col.ColumnName);
					createSqlBuilder.Append("\t");
					createSqlBuilder.Append(colDataType);
					createSqlBuilder.Append(",");

		 	 	 }
				createSqlBuilder.Length--;
				createSqlBuilder.Append(" );");		  			    
				string sql2 = createSqlBuilder.ToString();
				ArbiterCopyUtilLibrary.writeToLog("Running: "+sql2);
				SQLiteCommand command2 = new SQLiteCommand(sql2, liteConnect);
				command2.CommandTimeout = -1;
				command2.ExecuteNonQuery();
				command2.Dispose();
			
			}
			StringBuilder  tableColumnString  = new StringBuilder();
			  foreach (DataColumn column in arbiterCopyTables[index].Columns){
			  
			      tableColumnString.Append(column.ColumnName.ToString()).Append(",");
			 
			 }
			 tableColumnString.Length--;
			StringBuilder  insertBuilder      = new StringBuilder();
			
		   foreach (DataRow row in arbiterCopyTables[index].Rows){
		            insertBuilder.Append("INSERT INTO ").Append(sqliteTableName).Append("( ").Append(tableColumnString.ToString()).Append(" ) VALUES ( ");
			    foreach (DataColumn column in arbiterCopyTables[index].Columns){
			      
			      if(column.DataType.Name.ToString().Contains("TEXT") || column.DataType.Name.ToString().Contains("Char") || column.DataType.Name.ToString().Contains("String") ) {
			      
			        insertBuilder.Append("\'").Append(row[column].ToString()).Append(", ").Append("\'");
			    
			       } else{
			         insertBuilder.Append(row[column].ToString()).Append(", ");
			       }
			    
			      }
			      insertBuilder.Length--;
			      insertBuilder.Append(") ");
			      SQLiteCommand command3 = new SQLiteCommand(insertBuilder.ToString(), liteConnect);
			      command3.CommandTimeout = -1;
			      command3.ExecuteNonQuery();
			      command3.Dispose();
			      
			    }
			//	arbiterCopyTables[index].Clear();
			}	
		}catch(Exception e){
				ArbiterCopyUtilLibrary.writeToLog("Error saving table with  index "+index+" to SQLite. Message : " + e.Message);
				Console.WriteLine("Error saving table with  index "+index+" to SQLite. Message : " + e.Message);
				ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
				ArbiterCopyUtilLibrary.writeToLog(e.ToString());
				Console.WriteLine(e.StackTrace);
				Console.WriteLine(e.ToString());
				 
			  }
		}
		}


	public  static  void  loadDataFromSQLite(int  index){
	
		string dbFileName       =  getTempFilePath(index)+ "\\" + ArbiterCopyUtilLibrary.temporaryFileNamePrefix + "_" + index.ToString() + ".sqlite";
		string sqliteTableName  =  ArbiterCopyUtilLibrary.temporaryTableName+"_"+index.ToString();	
		if (File.Exists(dbFileName)){
		try{
                    using (SQLiteConnection liteConnect = new SQLiteConnection("Data Source=" + dbFileName + ";Version=3;"))
                    {
                        liteConnect.Open();
                        string sql = "SELECT  * FROM " + sqliteTableName + ";";
                        Console.WriteLine("Running: " + sql);
                        SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
                        arbiterCopyTables[index] = new DataTable();
                        SQLiteDataReader reader = command.ExecuteReader();
                        arbiterCopyTables[index].Load(reader);
		}
	}catch(Exception  e){
                    ArbiterCopyUtilLibrary.writeToLog("Error reading data for table with  index " + index + " from SQLite. Message : " + e.Message);
                    Console.WriteLine("Error reading data for table with " + index + " from SQLite. Message : " + e.Message);
                    ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
                    ArbiterCopyUtilLibrary.writeToLog(e.ToString());
                    Console.WriteLine(e.StackTrace);
                    Console.WriteLine(e.ToString());
		
	}

	}else{
	  Console.WriteLine("Could not find file "+dbFileName);
                ArbiterCopyUtilLibrary.writeToLog("Could not find file "+dbFileName);
	}
			}

	    public   void  runArbiterTransactionCopyFilter(string destinationServer,  string bulkQuery, int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString         = connectionStringMap[sourceServer];
			 	arbiterCopyFilterTables[tabInd]         = new DataTable();
				string  tableName                       = ArbiterCopyUtilLibrary.arbiterCopyFilterTablePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType+"_"+tabInd.ToString();
				try{
                lock (locker)
                {	Console.WriteLine("Running fetch script for table: "+tabInd.ToString());

					if( ArbiterCopyUtilLibrary.fs.BaseStream != null){
							ArbiterCopyUtilLibrary.writeToLog("Running fetch script for table: " + tabInd.ToString()+"\n"+bulkQuery);
					}
				}

                bulkCopyDataFromRemoteServer(bulkQuery, tableName);

			 }catch(Exception e){
						Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+arbiterCopyFilterTables[tabInd].ToString()+". Error: " + e.Message);
						Console.WriteLine(e.StackTrace);
						ArbiterCopyUtilLibrary.writeToLog("Error while running bulk insert script "+bulkQuery+": " + e.Message);
						ArbiterCopyUtilLibrary.writeToLog(e.ToString());
						ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
						Console.WriteLine(e.ToString());
						emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert script "+bulkQuery+": " + e.Message);
						emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
			
						}
				}
        public static void bulkCopyDataFromRemoteServer( string copyScript, string destTable)
        {

            createSQLTableFromDataTable(destTable, schemaTable, destServerConnectionString);
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
                    using (SqlConnection bulkCopyConnection = new SqlConnection(destServerConnectionString))
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

					}
               
            }
        }

public   void  exportDataToDestinationTable( int tabInd ){

				Console.WriteLine("Table  index: "+tabInd.ToString());

				string   targetConnectionString      = connectionStringMap["target"];
				string   stagingTableName            = ArbiterCopyUtilLibrary.arbiterCopyTableNamePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType+"_"+tabInd.ToString();
				string   filterTableName             = ArbiterCopyUtilLibrary.arbiterCopyFilterTablePrefix+"_"+ArbiterCopyUtilLibrary.arbiterCopyType+"_"+tabInd.ToString();
				
				string  stagingTableIndex     		 = "CREATE INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON ["+stagingTableName+"] (["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]);";
				string  filterTableIndex    		 = "CREATE INDEX [ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  ON ["+filterTableName+"] (["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]);";
				
				executeScript(stagingTableIndex+stagingTableIndex, connectionStringMap["destination"]);
				
				string  bulkInsertScript             = "SELECT  * FROM  ["+stagingTableName+"] WITH (NOLOCK, INDEX=ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+")  WHERE  ["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"] NOT IN (SELECT ["+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"]  FROM  ["+filterTableName+"]   WITH (NOLOCK, INDEX=ix_"+ArbiterCopyUtilLibrary.arbiterCopyFilterField+"))";
			  try{
                lock (locker)
                {	Console.WriteLine("Running fetch script for table: "+tabInd.ToString());

					if( ArbiterCopyUtilLibrary.fs.BaseStream != null){
							ArbiterCopyUtilLibrary.writeToLog("Running insert script for table: " + stagingTableName+"\n"+bulkQuery);
					}
				}
                bulkCopyDataToDestinationServer(bulkInsertScript, ArbiterCopyUtilLibrary.destinationTable, connectionStringMap["destination"]);
                
			}catch(Exception e){

						
					
								Console.WriteLine("Error while running bulk insert for table index: "+tabInd+" and name: "+arbiterCopyTables[tabInd].ToString()+". Error: " + e.Message);
								Console.WriteLine(e.StackTrace);
								ArbiterCopyUtilLibrary.writeToLog("Error while running bulk insert script "+bulkQuery+": " + e.Message);
								ArbiterCopyUtilLibrary.writeToLog(e.ToString());
								ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);
								Console.WriteLine(e.ToString());
								emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert script "+bulkQuery+": " + e.Message);
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
					
						}
				}
		        public static void bulkCopyDataToDestinationServer( string copyScript, string destTable, string connectionStr)
        {

    
           try
            {
                using (SqlConnection serverConnection = new SqlConnection(connectionStr))
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
                    using (SqlConnection bulkCopyConnection = new SqlConnection(connectionStr))
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.UseInternalTransaction, null))
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
								bulkCopyDataToDestinationServer( copyScript,  destTable,connectionStr); 
								emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {

							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							ArbiterCopyUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							ArbiterCopyUtilLibrary.writeToLog(e.StackTrace);

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
            string targetConnectionString = sourceServerConnectionString.Replace("Network Library=DBMSSOCN", "Provider=SQLOLEDB");
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
		public static void  createFinalView(int totalTables){

		    StringBuilder  viewBuilder  = new StringBuilder();
            executeScript("IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[" + ArbiterCopyUtilLibrary.reportTableName + "]') AND type in (N'V')) BEGIN  DROP VIEW [" + ArbiterCopyUtilLibrary.reportTableName + "] END ",destServerConnectionString);
            viewBuilder.Append(" CREATE  VIEW [").Append(ArbiterCopyUtilLibrary.reportTableName).Append("]  AS ");
			for (int i = 0; i < totalTables; i++ ){
                viewBuilder.Append(" SELECT * FROM ").Append(ArbiterCopyUtilLibrary.temporaryTableName+"_"+i.ToString()).Append(" WITH (NOLOCK) UNION ALL ");
			}
            viewBuilder.Length= viewBuilder.Length -11;
            viewBuilder.Append(";");
            executeScript(viewBuilder.ToString(), destServerConnectionString);

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