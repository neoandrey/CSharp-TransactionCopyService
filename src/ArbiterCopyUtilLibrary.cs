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

namespace ArbiterCopyService{
       public class   ArbiterCopyUtilLibrary{

                public  static string 					    sourceServer        				 = "";
                public  static string 					    sourceDatabase       				 =    "";
                public  static int 					        sourcePort           				 =    1433;
                public static  string                       destinationServer 			         =    "";	
                public static  string                       destinationDatabase       	         =    "";
                public static  int                          destinationPort                 	 =     0;
               public  static  string                       destinationTable                     =     "";
                public  static System.IO.StreamWriter 	    fs;
                public  static string 					    logFile								 = Directory.GetCurrentDirectory()+"\\log\\report_generator_"+DateTime.Now.ToString("yyyyMMdd_HH_mm_ss")+".log";
                public  static string 					    configFileName                       = Directory.GetCurrentDirectory()+"\\conf\\report_generator_config.json";
                public  static int  					    batchSize       				     = 100;

                public  static string                       arbiterCopyTableNamePrefix           = "";

                public static  string                       arbiterCopyFilterTablePrefix         = "";

                public static  string                       arbiterCopyType                      = "";
  
                public static  int                          arbiterCopyMode                      = 0;

                public static ArrayList                     arbiterCopySpecificParamterValues    =  new ArrayList();

                public static string                        copyStartParameter                   =  "";

                public static string                        copyEndParameter                     =  "";
            
                public static string                        copyStartParameterValue              =  "";

                public static string                        copyEndParameterValue                =  "";
            
                public static string                        arbiterCopyFilterScript              =  "";

                public static string                        copyScript                           =    "";

                public  static string 				        toAddress                            = "";

                 public  static string 				        fromAddress   	    				 = "ArbiterCopy@interswitchgroup.com";
                public  static string 					    bccAddress    	   					 = "";
     
                public  static string 					    ccAddress     	   				     = "";
                public  static string 					    smtpServer    						 = "172.16.10.223";
                public  static int 					        smtpPort     	    			     = 25;
                public  static string 					    sender             					 = "ArbiterCopy@interswitchgroup.com";
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
                public  static ArbiterCopyConfiguration     arbiterConfig                          =  new ArbiterCopyConfiguration();
                public static  int                          WAIT_INTERVAL                        =   1000;
                public static String                        temporaryTableName                   =   "temp_report_table";
                public  static int                          concurrentThreads                    =  1;
		        public  static ConnectionProperty 		    sourceConnectionProps;
                public  static ConnectionProperty 		    destinationConnectionProps;

                public static  string                       arbiterFilterCopyScript             =   "";
  
                public   static   int                       numOfDaysFromStart                  =         0          ;
                public static readonly object               locker                              = new object();  

                public static  string                       arbiterCopyFilterField              = "";

                 public   ArbiterCopyUtilLibrary(){
                        initArbiterCopyUtilLibrary();

                }
      			public   ArbiterCopyUtilLibrary(string  cfgFile){
					
					   if(!string.IsNullOrEmpty(cfgFile) ){

						   string  nuCfgFile  = "";
                           Console.WriteLine("Logging report activities to file: "+logFile);
                           Console.WriteLine("");
						   Console.WriteLine("Loading configurations in  configuration file: "+cfgFile);
						   nuCfgFile           =  cfgFile.Contains("\\\\")? cfgFile:cfgFile.Replace("\\", "\\\\");

						   try{
							   if(File.Exists(nuCfgFile)){

								configFileName     = nuCfgFile;
								initArbiterCopyUtilLibrary();

							   }
						   }catch(Exception e){
							    
								Console.WriteLine("Error reading configuration file: "+e.Message);
								Console.WriteLine(e.StackTrace);
								writeToLog("Error reading configuration file: "+e.Message);
								writeToLog(e.StackTrace);
							
						   }
					   }
				 	
		       	         		
				}
                public  void  initArbiterCopyUtilLibrary(){

					if (!File.Exists(logFile))  {
                        
							fs = File.CreateText(logFile);
					
                    }else{
					
                    		fs = File.AppendText(logFile);
					
                    } 
					log("===========================Started Report Generator Session at "+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"==============================");
					readConfigFile(configFileName);

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
                }
				public  static  void readConfigFile(string configFileName){					
				    writeToLog("Reading configurations from "+configFileName+"  ...");
                    Console.WriteLine("Reading configurations from "+configFileName+"  ...");
                    try{

					    string  propertyString            = File.ReadAllText(configFileName);
                        arbiterConfig                     = Newtonsoft.Json.JsonConvert.DeserializeObject<ArbiterCopyConfiguration>(propertyString);  
                        sourceServer 			          = arbiterConfig.source_server;       	
                        sourceDatabase       		      = arbiterConfig.source_database;
                        sourcePort           		      = arbiterConfig.source_port;
                        destinationServer 			      = arbiterConfig.destination_server;       	
                        destinationDatabase       	      = arbiterConfig.destination_database;
                        destinationPort           	      = arbiterConfig.destination_port;
                        batchSize       		          = arbiterConfig.batch_size;
                        arbiterCopyTableNamePrefix        = arbiterConfig.arbiter_copy_table_name_prefix;
                        arbiterCopyFilterTablePrefix      = arbiterConfig.arbiter_copy_filter_table_prefix;
                        arbiterCopyType                   = arbiterConfig.arbiter_copy_type;
                        arbiterCopyMode                   = arbiterConfig.arbiter_copy_mode;
                        arbiterCopySpecificParamterValues = arbiterConfig.arbiter_copy_specific_parameter_values;
                        copyStartParameter                = arbiterConfig.copy_start_parameter;
                        copyEndParameter                  = arbiterConfig.copy_end_parameter;
                        copyStartParameterValue           = arbiterConfig.copy_start_parameter_value;
                        copyEndParameterValue             = arbiterConfig.copy_end_parameter_value;
                        arbiterFilterCopyScript           = arbiterConfig.arbiter_copy_filter_script;
                        copyScript                        = arbiterConfig.copy_script;
                        toAddress                         = arbiterConfig.to_address;
                        fromAddress   	    	          = arbiterConfig.from_address;
                        bccAddress    	   	              = arbiterConfig.bcc_address;              
                        ccAddress     	   	              = arbiterConfig.cc_address;
                        smtpServer    		              = arbiterConfig.smtp_server;
                        smtpPort     	    		      = arbiterConfig.smtp_port;
                        sender             		          = arbiterConfig.sender;
                        senderPassword 	   	              = arbiterConfig.sender_password;
                        isSSLEnabled  		              = arbiterConfig.is_ssl_enabled;                     
                        alternateRowColour                = arbiterConfig.alternate_row_colour;
                        emailFontFamily                   = arbiterConfig.email_font_family;
                        emailFontSize                     = arbiterConfig.email_font_size;
                        colour                            = arbiterConfig.colour;
                        borderColour                      = arbiterConfig.border_colour;
                        sendEmailNotification             = arbiterConfig.send_email_notification; 
                        WAIT_INTERVAL                     = arbiterConfig.wait_interval;
                        arbiterCopyFilterField            = arbiterConfig.arbiter_copy_filter_field;

                        Console.WriteLine("sourceServer: "+sourceServer);
                        Console.WriteLine("sourceDatabase: "+sourceDatabase);
						Console.WriteLine("Configurations have been successfully initialised.");
                        writeToLog("Configurations have been successfully initialised.");

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