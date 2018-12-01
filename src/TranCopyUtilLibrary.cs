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
using Newtonsoft.Json.Linq;

namespace TranCopyService{
       public class   TranCopyUtilLibrary{

                public  static string 					    sourceServer        				 = "";
                public  static string 					    sourceDatabase       				 =    "";
                public  static int 					        sourcePort           				 =    1433;
                public static  string                       destinationServer 			         =    "";	
                public static  string                       destinationDatabase       	         =    "";
                public static  int                          destinationPort                 	 =     0;
               public  static  string                       destinationTable                     =     "";
                public  static System.IO.StreamWriter 	    fs;
                public  static string 					    logFile								 = Directory.GetCurrentDirectory()+"\\log\\copy_log_for_"+DateTime.Now.ToString("yyyyMMdd_HH_mm_ss")+".log";
                public  static string 					    configFileName                       = Directory.GetCurrentDirectory()+"\\conf\\copy_config.json";
                public  static int  					    batchSize       				     = 100;

                public  static string                       tranCopyTableNamePrefix           = "";

                public static  string                       tranCopyFilterTablePrefix         = "";

                public static  string                       tranCopyType                      =  "";
  
                public static  int                          tranCopyMode                      = 0;

                public static ArrayList                     tranCopySpecificParamterValues    =  new ArrayList();

                public static string                        copyStartParameter                   =  "";

                public static string                        copyEndParameter                     =  "";
            
                public static string                        copyStartParameterValue              =  "";

                public static string                        copyEndParameterValue                =  "";
            
                public static string                        tranCopyFilterScript              =  "";

                public static string                        copyScript                           =    "";

                public  static string 				        toAddress                            = "";

                 public  static string 				        fromAddress   	    				 = "TranCopy@interswitchgroup.com";
                public  static string 					    bccAddress    	   					 = "";
     
                public  static string 					    ccAddress     	   				     = "";
                public  static string 					    smtpServer    						 = "172.16.10.223";
                public  static int 					        smtpPort     	    			     = 25;
                public  static string 					    sender             					 = "TranCopy@interswitchgroup.com";
                public  static string 					    senderPassword 	   					 = "";
                public  static bool 					    isSSLEnabled  					     = false;
                public  static string                       alternateRowColour                   = "#cce0ff";
                public  static string                       emailFontFamily                      = "arial,times new roman,verdana,sans-serif;";
                public  static string                       emailFontSize                        = "11px";
                public  static string                       colour                               = "#333333";
                public  static string                       borderColour                         = "gray";
                public  static bool                         attachReportToMail                   =  false;
                public  static bool                         embedReportInMail                    =  false;
                public  static bool                         sendEmailNotification                =  false;
                public  static TranCopyConfiguration     tranConfig                          =  new TranCopyConfiguration();
                public static  int                          WAIT_INTERVAL                        =   1000;
                public static String                        temporaryTableName                   =   "temp_report_table";
                public  static int                          concurrentThreads                    =  1;
		        public  static ConnectionProperty 		    sourceConnectionProps;
                public  static ConnectionProperty 		    destinationConnectionProps;

                public  static ConnectionProperty 		    stagingConnectionProps;

                public static  string                       tranFilterCopyScript             =   "";
  
                public   static   int                       numOfDaysFromStart                  =         0          ;
                public static readonly object               locker                              = new object();  

                public static  string                       tranCopyFilterField              = "";

                public static string                        emailSeparator                        = "";
                
                public static string                        borderWidth                          =  "";

                public static string                        headerBgColor                        =   "";

                public static  bool                         cleanUpAfterCopy                     =  false;

                public static  string                       stagingServer                        =  "";

                public static  string                       stagingDatabase                      = "";

                public static  int                          stagingPort                          =  1433;

                public static  ArrayList                    destinationTableColumnOrder           =  new ArrayList();

                public  static   bool                       sendMailOnError                       = true;

                public  static  string                      finalSelectFields                     = "";

                public static   string                      filteredStagingTablePrefix            = "";

                public static  string                       tableMergeScript                      = "";

                public static  bool                         showParameterTable                    = true;            

                public static  string                       emailSubject                          = "";
 
                 public   TranCopyUtilLibrary(){

                        initTranCopyUtilLibrary();

                }
      			public   TranCopyUtilLibrary(string  cfgFile){
					
					   if(!string.IsNullOrEmpty(cfgFile) ){

						   string  nuCfgFile  = "";
                           Console.WriteLine("Logging report activities to file: "+logFile);
                           Console.WriteLine("");
						   Console.WriteLine("Loading configurations in  configuration file: "+cfgFile);
						   nuCfgFile           =  cfgFile.Contains("\\\\")? cfgFile:cfgFile.Replace("\\", "\\\\");

						   try{
							   if(File.Exists(nuCfgFile)){

								configFileName     = nuCfgFile;
								initTranCopyUtilLibrary();

							   }
						   }catch(Exception e){
							    
								Console.WriteLine("Error reading configuration file: "+e.Message);
								Console.WriteLine(e.StackTrace);
								writeToLog("Error reading configuration file: "+e.Message);
								writeToLog(e.StackTrace);
							
						   }
					   }
				 	
		       	         		
				}
                public  void  initTranCopyUtilLibrary(){

					readConfigFile(configFileName);

                    if (!File.Exists(logFile))  {
                        
							fs = File.CreateText(logFile);
					
                    }else{
					
                    		fs = File.AppendText(logFile);
					
                    } 
                    
					log("===========================Started Tran Copy for "+tranCopyType+" Session at "+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"==============================");
					   writeToLog("Configurations have been successfully initialised.");
                    

                    Console.WriteLine("sourceServer: "+sourceServer);
                    Console.WriteLine("sourceDatabase: "+sourceDatabase);
				    if (!String.IsNullOrEmpty(sourceServer) &&  !String.IsNullOrEmpty(sourceDatabase)){

                          sourceConnectionProps      = new ConnectionProperty( sourceServer, sourceDatabase );

                    } else {

                        Console.WriteLine("Source connection details are not complete");
                        writeToLog("Source connection details are not complete");

                    }

                    if (!String.IsNullOrEmpty(destinationServer) &&  !String.IsNullOrEmpty(destinationDatabase)){
    
                        destinationConnectionProps  = new ConnectionProperty( destinationServer, destinationDatabase );

                    } else {

                        Console.WriteLine("Source connection details are not complete");
                        writeToLog("Destination connection details are not complete");

                    }   

                    if (!String.IsNullOrEmpty(stagingServer) &&  !String.IsNullOrEmpty(stagingDatabase)){
    
                        stagingConnectionProps  = new ConnectionProperty( stagingServer, stagingDatabase );

                    } else {

                        Console.WriteLine("Staging connection details are not complete");
                        writeToLog("Staging connection details are not complete");

                    }                 
                }
				public  static  void readConfigFile(string configFileName){					
				   // writeToLog("Reading configurations from "+configFileName+"  ...");
                    Console.WriteLine("Reading configurations from "+configFileName+"  ...");
                    try{

					    string  propertyString            = File.ReadAllText(configFileName);
                        tranConfig                     = Newtonsoft.Json.JsonConvert.DeserializeObject<TranCopyConfiguration>(propertyString);  
                        sourceServer 			          = tranConfig.source_server;       	
                        sourceDatabase       		      = tranConfig.source_database;
                        sourcePort           		      = tranConfig.source_port;
                        destinationServer 			      = tranConfig.destination_server;       	
                        destinationDatabase       	      = tranConfig.destination_database;
                        destinationPort           	      = tranConfig.destination_port;
                        batchSize       		          = tranConfig.batch_size;
                        destinationTable                  = tranConfig.destination_table;
                        tranCopyTableNamePrefix        = tranConfig.copy_table_name_prefix;
                        tranCopyFilterTablePrefix      = tranConfig.copy_filter_table_prefix;
                        tranCopyType                   = tranConfig.copy_type;
                        logFile						      = Directory.GetCurrentDirectory()+"\\log\\copy_log_for_"+tranCopyType.ToLower()+"_"+DateTime.Now.ToString("yyyyMMdd_HH_mm_ss")+".log";
                        tranCopyMode                   = tranConfig.copy_mode;
                        tranCopySpecificParamterValues = tranConfig.copy_specific_parameter_values;
                        copyStartParameter                = tranConfig.copy_start_parameter;
                        copyEndParameter                  = tranConfig.copy_end_parameter;
                        copyStartParameterValue           = tranConfig.copy_start_parameter_value;
                        copyEndParameterValue             = tranConfig.copy_end_parameter_value;
                        tranFilterCopyScript           = tranConfig.copy_filter_script;
                        copyScript                        = tranConfig.copy_script;
                        toAddress                         = tranConfig.to_address;
                        fromAddress   	    	          = tranConfig.from_address;
                        bccAddress    	   	              = tranConfig.bcc_address;              
                        ccAddress     	   	              = tranConfig.cc_address;
                        smtpServer    		              = tranConfig.smtp_server;
                        smtpPort     	    		      = tranConfig.smtp_port;
                        sender             		          = tranConfig.sender;
                        senderPassword 	   	              = tranConfig.sender_password;
                        isSSLEnabled  		              = tranConfig.is_ssl_enabled;                     
                        alternateRowColour                = tranConfig.alternate_row_colour;
                        emailFontFamily                   = tranConfig.email_font_family;
                        emailFontSize                     = tranConfig.email_font_size;
                        colour                            = tranConfig.color;
                        borderColour                      = tranConfig.border_color;
                        sendEmailNotification             = tranConfig.send_email_notification; 
                        WAIT_INTERVAL                     = tranConfig.wait_interval;
                        tranCopyFilterField            = tranConfig.copy_filter_field;
                        numOfDaysFromStart                = tranConfig.num_of_previous_days_from_start;
                        emailSeparator                    = tranConfig.email_separator;
                        borderWidth                       = tranConfig.border_width;
                        headerBgColor                     = tranConfig.header_background_color;
                        cleanUpAfterCopy                  = tranConfig.clean_up_after_copy;
                        stagingServer                     = tranConfig.staging_server;
                        stagingDatabase                   = tranConfig.staging_database;
                        stagingPort                       = tranConfig.staging_port;
                        destinationTableColumnOrder       = tranConfig.destination_table_column_order;
                        sendMailOnError                   = tranConfig.send_mail_on_error;    
                        finalSelectFields                 = tranConfig.final_select_fields; 
                        filteredStagingTablePrefix        = tranConfig.filtered_staging_table_prefix;
                        tableMergeScript                  = tranConfig.table_merge_script;  
                        showParameterTable                = tranConfig.show_parameters_in_mail; 
                        emailSubject                      = tranConfig.email_subject; 

						Console.WriteLine("Configurations have been successfully initialised.");                  

                }catch(Exception e){

                    Console.WriteLine("Error reading configuration file: "+e.Message);
                    Console.WriteLine(e.StackTrace);
                    writeToLog("Error reading configuration file: "+e.Message);
                    writeToLog(e.StackTrace);

                }
            
            }
            public static void  writeToLog(string logMessage){
                lock (locker)
                {
                    fs.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"=>"+logMessage);
                }
            }
			 public static Dictionary<string,string>readJSONMap(ArrayList rawMap){

                    Dictionary<string, string> tempDico = new  Dictionary<string, string>();
                    string tempVal  ="";
                    if(rawMap!=null)
                    foreach(var keyVal in rawMap){
                                
                                   tempVal = keyVal.ToString();
                                   if(!string.IsNullOrEmpty(tempVal)){
                                        tempVal = tempVal.Replace("{","").Replace("}","").Replace("\"","").Trim();
                                        Console.WriteLine("tempVal: "+tempVal);
                                       if(tempVal.Split(':').Count() ==2)tempDico.Add(tempVal.Split(':')[0].Trim(),tempVal.Split(':')[1].Trim());  
                                   }  

                    }
                return tempDico;
            }

			public static  void log(string logMessage){
				fs.WriteLine(logMessage);
				fs.Flush();
			}
			public static void closeLogFile(){
				fs.Close();
			}
}

}