using System;
using System.Collections;
using System.Text;
using System.Data;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Data.OleDb;
using System.Configuration;
using System.Threading;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Data.SQLite;
using System.Data.DataSetExtensions;
using System.ServiceProcess;


namespace genericservicescheduler{
	
  public class  TemplateServiceWrapper : ServiceBase{

              string serviceName        			  		   =   "";
              static string exe                			  	   =   "";
              string parameterString    		      		   =   "";
              private static System.Timers.Timer timer1        =   null;
			  static  string  liteConnectionString             =   "";
              Int32 interval             			  		   =    1000;
			  static string    getServiceInformationScript     =   "SELECT  s.schedule_id,s.service_id, schedule_name,schedule_create_date ,start_time,start_date ,start_time_hh,start_time_mm,start_time_ss,start_time_am_pm,end_date, CASE  WHEN repeat_every is NULL THEN '' ELSE repeat_every END AS frequency, repeat_every,repeat_type, CASE  WHEN frequency is NULL THEN '' ELSE frequency END AS frequency ,frequency_type,s.description,enabled,run_indefinitely,last_modified_date,schedule_run_id,schedule_run_status,schedule_start_time, schedule_end_date, schedule_last_run_time, schedule_next_run_time, schedule_run_id FROM service_schedule_table s JOIN  service_table t ON t.service_id = s.service_id  LEFT JOIN service_run_history h on s.schedule_id = h.schedule_id AND service_name = 'service_name_val'   ORDER BY schedule_run_id DESC LIMIT 1";
			  public  static DateTime  currentDateTime 		   =   DateTime.Now;
			  int delay               			 	           =   0;
			  string  addServiceRunData                        =   "INSERT INTO service_run_history( schedule_id,service_id,schedule_run_status,schedule_start_time,schedule_end_date,schedule_last_run_time,schedule_next_run_time ) VALUES ('schedule_id_val','service_id_val','schedule_run_status_val','schedule_start_time_val','schedule_end_date_val','schedule_last_run_time_val','schedule_next_run_time_val')";
              string  updateServiceRunStatus                   =   "UPDATE service_run_history SET schedule_run_status = 'schedule_run_status_val'   WHERE  schedule_run_id = 'schedule_run_id_val'";
              static string  logInsertScript                   =   "INSERT INTO  service_wrapper_log  (log_time, log_message) VALUES ('log_time_val', 'log_message_val')";
              DataTable    servScheduleTab                     =   new DataTable();
              long  maxID                                      =   0;
			  DateTime				 lastRunDateTime 		   =   DateTime.Now;
			  DateTime				 nextRunDateTime 		   =   DateTime.Now;
			  DateTime				 expectedRunTime           =   DateTime.Now;
              private int eventId                              =   1;
              System.Diagnostics.EventLog  eventLog1;

              public TemplateServiceWrapper(){
                
              }

            public enum ServiceState  
            {  
                SERVICE_STOPPED 			= 0x00000001,  
                SERVICE_START_PENDING	    = 0x00000002,  
                SERVICE_STOP_PENDING 		= 0x00000003,  
                SERVICE_RUNNING 			= 0x00000004,  
                SERVICE_CONTINUE_PENDING    = 0x00000005,  
                SERVICE_PAUSE_PENDING 		= 0x00000006,  
                SERVICE_PAUSED 				= 0x00000007,  
            }  

            [StructLayout(LayoutKind.Sequential)]  
            public struct ServiceStatus  
            {  
                public int dwServiceType;  
                public ServiceState dwCurrentState;  
                public int dwControlsAccepted;  
                public int dwWin32ExitCode;  
                public int dwServiceSpecificExitCode;  
                public int dwCheckPoint;  
                public int dwWaitHint;  
            };
            
			 public static void  initLiteConnectionString(string exe2){
                        string  locationPrefix =  exe2.Substring(0, exe2.LastIndexOf('\\'))+"\\";
                        liteConnectionString =  "Data Source="+locationPrefix+"..\\db\\scheduler_db.sqlite;Version=3;";

             }

                 public static string reformatDate(string  rawDate) {

                   writeToLog("reformatting: "+rawDate);
                    string  finalDate   ="";
                    if(!rawDate.Contains("-")){
                          string[] dateComps  = rawDate.Split(' ');
                          string[] dayComps   = dateComps[0].Split('/');
                         finalDate   = dayComps[2]+"-"+dayComps[1].PadLeft(2,'0')+"-"+dayComps[0].PadLeft(2,'0')+" "+dateComps[1]+" "+dateComps[2];
                    }else{
                         finalDate   = rawDate;
                    }
                    return  finalDate;
             }
				
            [DllImport("advapi32.dll", SetLastError=true)]  
             private static extern bool SetServiceStatus(IntPtr handle, ref ServiceStatus serviceStatus);  

             public TemplateServiceWrapper(string service,string serviceParams,  string serviceExe){
                 try{    
                            eventLog1 = new System.Diagnostics.EventLog();
                            ((System.ComponentModel.ISupportInitialize)(eventLog1)).BeginInit();
                                 eventLog1.Source = service;
                                 eventLog1.Log = service+"_logs";  
                            ((System.ComponentModel.ISupportInitialize)(eventLog1)).EndInit();
                            if (!System.Diagnostics.EventLog.SourceExists(service)) {         
                                 System.Diagnostics.EventLog.CreateEventSource(service,service+"_logs");
                            }

                            this.CanStop               = true;
                            this.CanPauseAndContinue   = false;
                            this.AutoLog               = true;
                 

							
							serviceName                = service;
							exe                        = serviceExe;
							parameterString            = serviceParams;
			
						    servScheduleTab = getDataFromStore(getServiceInformationScript.Replace("service_name_val",serviceName));
                            string  schID   = string.IsNullOrEmpty(servScheduleTab.Rows[0]["schedule_run_id"].ToString())?"0":servScheduleTab.Rows[0]["schedule_run_id"].ToString();
                            maxID           = int.Parse(schID) +1;
                         
							
                            if (servScheduleTab.Rows.Count > 0 ){	
							
                    
                                 string   lastRunTime             		     =   string.IsNullOrEmpty(servScheduleTab.Rows[0]["schedule_last_run_time"].ToString())?DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss tt"):servScheduleTab.Rows[0]["schedule_last_run_time"].ToString();
                                 string   nextRunTime             		     =   string.IsNullOrEmpty(servScheduleTab.Rows[0]["schedule_next_run_time"].ToString())?servScheduleTab.Rows[0]["start_time"].ToString():servScheduleTab.Rows[0]["schedule_next_run_time"].ToString();
                                 string  repeatType    					 	 =   "";
                                 string  frequencyType 					     =   "";
                                 string  frequencyText 					     =   "";
                                 string  repeatText    					 	 =   "";
								 currentDateTime 					     	 =   DateTime.Now;
								 lastRunDateTime 				 	         =   lastRunTime.Contains("/")? Convert.ToDateTime(reformatDate(lastRunTime)):Convert.ToDateTime(reformatDate(lastRunTime));
								 nextRunDateTime 				 	         =   nextRunTime.Contains("/")? Convert.ToDateTime(reformatDate(nextRunTime)):Convert.ToDateTime(reformatDate(nextRunTime));
								 expectedRunTime                             =   DateTime.Now;



                                foreach (DataRow row in servScheduleTab.Rows) {

                                 if(!string.IsNullOrEmpty(row["repeat_type"].ToString())  && row["repeat_type"].ToString()    != "DISABLED"){

                                     repeatType              				 =   row["repeat_type"].ToString();
                                     repeatText              				 =   string.IsNullOrEmpty(row["repeat_every"].ToString())?"0":row["repeat_every"].ToString();
                                     lastRunTime             				 =   string.IsNullOrEmpty(row["schedule_last_run_time"].ToString())?DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss tt"):row["schedule_last_run_time"].ToString();
                                     nextRunTime             				 =   string.IsNullOrEmpty(row["schedule_next_run_time"].ToString())?row["start_time"].ToString():row["schedule_next_run_time"].ToString();
									 frequencyText          			     =   string.IsNullOrEmpty(row["frequency"].ToString())?"0":row["frequency"].ToString();
									 frequencyType           			     =   row["frequency_type"].ToString();
                                     int repeat             				 =   int.Parse(repeatText.Trim());
	                                 lastRunDateTime 				 	     =   lastRunTime.Contains("/")? Convert.ToDateTime(reformatDate(lastRunTime)):Convert.ToDateTime(reformatDate(lastRunTime));
								     nextRunDateTime 				 	     =   nextRunTime.Contains("/")? Convert.ToDateTime(reformatDate(nextRunTime)):Convert.ToDateTime(reformatDate(nextRunTime));
									 delay               				     =   0;
									 expectedRunTime                         =   DateTime.Now;

                                     if(string.IsNullOrEmpty(nextRunTime)){							
										 
                                            if(repeatType=="hour(s)"){

                                                    TimeSpan time 			 = new TimeSpan( repeat, 0, 0);
											        expectedRunTime = lastRunDateTime.Add(time);
													interval                 = repeat*60*60;

                                            }else if(repeatType=="minutes(s)"){
												    TimeSpan time = new TimeSpan( 0,repeat, 0);
													 expectedRunTime = lastRunDateTime.Add(time);
													interval                 = repeat*60;

                                            }else if  (repeatType=="second(s)"){
													TimeSpan time = new TimeSpan( 0,0, repeat);
												    expectedRunTime = lastRunDateTime.Add(time);
													interval                 = repeat;

                                            }
										   writeToLog("expectedRunTime: "+expectedRunTime.ToString());
                                            writeToLog("currentDateTime: "+currentDateTime.ToString());

                                            
                                            if(currentDateTime>expectedRunTime){
                                                
                                                expectedRunTime  =currentDateTime;

                                            }
                                              delay  = (int)Math.Ceiling(decimal.Parse(expectedRunTime.Subtract(currentDateTime).TotalSeconds.ToString().Trim()));
                                            writeToLog("delay: "+delay);
																						
                                        }else{

                                            if(repeatType=="hour(s)"){

                                                 
													interval                 = repeat*60*60;

                                            }else if(repeatType=="minutes(s)"){
											
													interval                 = repeat*60;

                                            }else if  (repeatType=="second(s)"){
											
													interval                 = repeat;

                                            }
                                            
										   writeToLog("1. nextRunDateTime: "+nextRunDateTime.ToString());
                                           writeToLog("2. currentDateTime: "+currentDateTime.ToString());
                                                                                        
                                            if(currentDateTime>nextRunDateTime){
                                                
                                                nextRunDateTime  =currentDateTime;

                                            }
                                            writeToLog("Time Diff: "+nextRunDateTime.Subtract(currentDateTime).TotalSeconds.ToString().Trim());
										    delay  = (int)Math.Ceiling(decimal.Parse(nextRunDateTime.Subtract(currentDateTime).TotalSeconds.ToString().Trim()));
                                            writeToLog("delay: "+delay);

                                        }

                                 }else if(!string.IsNullOrEmpty(row["frequency_type"].ToString()) && row["frequency_type"].ToString() != "DISABLED"){

                                            frequencyText   =   row["frequency"].ToString();
                                            frequencyType   =   row["frequency_type"].ToString();
                                            lastRunTime     =   row["schedule_last_run_time"].ToString();
                                            nextRunTime     =   row["schedule_next_run_time"].ToString();
									
								
										     if(frequencyType      == "daily"){

													interval                 =  24*60*60/Int32.Parse(frequencyText);

                                            }else if(frequencyType == "weekly"){
											
													interval                 =  7*24*60*60/Int32.Parse(frequencyText);

                                            }else if  (frequencyType == "monthly"){
											
													interval                 = 30*24*60*60/Int32.Parse(frequencyText);

                                            }else if  (frequencyType =="yearly"){
											
													interval                 = 365*24*60*60/Int32.Parse(frequencyText);
                                            }
											
										if(string.IsNullOrEmpty(nextRunTime)){
											
											  delay     =  0;
											
									}else{

                                            if(currentDateTime>nextRunDateTime){

                                                nextRunDateTime  =currentDateTime;

                                            }
                                            
                                            delay  = (int)Math.Ceiling(decimal.Parse(nextRunDateTime.Subtract(currentDateTime).TotalSeconds.ToString().Trim()));
                                            
										
									}

									

                                 }else{
											 writeToLog("Could not find a schedule for service: "+serviceName);
								 
                                 }
                                }
							
                               this.Start();
                            }	

                            }catch(Exception e){
			 
                                writeToLog(e.Message);
                                writeToLog(e.StackTrace);
    		 
		                     }				
                  
              }

             public void Start(){


                 OnStart( new string[0]);
             }
   
                        public void OnTimer(object sender, System.Timers.ElapsedEventArgs args)  
                {  
                    
                     eventLog1.WriteEntry(serviceName+" logging event....", EventLogEntryType.Information, eventId++); 
                     writeToLog(serviceName+" logging event...."+". "+EventLogEntryType.Information+". "+eventId++); 
                }  

            public void  runService(object sender,  System.Timers.ElapsedEventArgs e){

               string endDateTime          =  servScheduleTab.Rows[0]["end_date"].ToString() ;
               string defaultEndDate       =  "1970-01-01 12:00:00 AM" ;
               bool isIndefinite           =  servScheduleTab.Rows[0]["run_indefinitely"].ToString()=="1"?true:false;
               bool enabled                =  servScheduleTab.Rows[0]["enabled"].ToString()=="1"?true:false;
               string lastRunStr           =  string.IsNullOrEmpty( servScheduleTab.Rows[0]["schedule_last_run_time"].ToString())?DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss tt"): servScheduleTab.Rows[0]["schedule_last_run_time"].ToString();
			   DateTime endTime            =  endDateTime ==defaultEndDate?DateTime.Now.AddDays(1): Convert.ToDateTime(endDateTime);
    
               eventLog1.WriteEntry("endDateTime: "+endDateTime+"-> "+nextRunDateTime);
               writeToLog("endDateTime: "+endDateTime+"-> "+nextRunDateTime);
               
               if(enabled){ 
                 if (endDateTime ==defaultEndDate ||    endTime>=nextRunDateTime){
                    if((isIndefinite) ||  (string.IsNullOrEmpty(lastRunStr) && !isIndefinite) ){
                            
                            try {


                                 executeOnDataStore(addServiceRunData.Replace("schedule_id_val",servScheduleTab.Rows[0]["schedule_id"].ToString())
                                                                    .Replace("service_id_val",servScheduleTab.Rows[0]["service_id"].ToString())
                                                                    .Replace("schedule_run_status_val","RUNNING")
                                                                    .Replace("schedule_start_time_val",reformatDate(servScheduleTab.Rows[0]["start_time"].ToString()))
                                                                    .Replace("schedule_end_date_val",reformatDate(servScheduleTab.Rows[0]["end_date"].ToString()))
                                                                    .Replace("schedule_last_run_time_val",reformatDate(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss tt")))
                                                                    .Replace("schedule_next_run_time_val",reformatDate(DateTime.Now.AddSeconds(interval).ToString("yyyy-MM-dd HH:mm:ss tt")))

                                );
 
                                    exe = exe.Replace("\'","");
                                    eventLog1.WriteEntry(" Process file: "+exe);
                                    eventLog1.WriteEntry(" parameters: "+parameterString);

                                    Process commandStr = new Process();
                                    if(exe.EndsWith("cmd") ||exe.EndsWith("bat") ||exe.EndsWith("vbs")||exe.EndsWith("exe")){
                                       commandStr.StartInfo.FileName = exe; 
                                       commandStr.StartInfo.Arguments =parameterString;
                                    }else {
                                              commandStr.StartInfo.FileName = "cmd";
                                              commandStr.StartInfo.Arguments ="/c "+exe+" "+parameterString;
                                    }
                                    
                                    commandStr.StartInfo.RedirectStandardInput = true;
                                    commandStr.StartInfo.RedirectStandardOutput = true;
                                    commandStr.StartInfo.CreateNoWindow = true;
                                    commandStr.StartInfo.UseShellExecute = false;
                                    commandStr.Start();
                                    commandStr.WaitForExit();
                                    string result = commandStr.StandardOutput.ReadToEnd();
                                    commandStr.StandardOutput.Close();             
                                    commandStr.StandardInput.Close();
                                    result.Trim();
                                    eventLog1.WriteEntry(result);
                                    eventLog1.WriteEntry(result);
                                    executeOnDataStore(updateServiceRunStatus.Replace("schedule_run_status_val","SUCCESS").Replace("schedule_run_id_val", maxID.ToString())
                                                    );

                            } catch(Exception ex){

                                            eventLog1.WriteEntry(ex.Message);
                                            eventLog1.WriteEntry(ex.StackTrace);
                                             executeOnDataStore(updateServiceRunStatus.Replace("schedule_run_status_val","FAILURE").Replace("schedule_run_id_val", maxID.ToString()));

                            }
                         }else{

                               eventLog1.WriteEntry("Serivce was schedule to run once and has already run");

                    }
                    }else{
                         eventLog1.WriteEntry("End date of service has been  reached");

                    }
                    
                    }else{
                         eventLog1.WriteEntry("The current schedule has not been enabled.");

                    }


    }
            protected override void OnStart(string[]  args){

                timer1                  = new System.Timers.Timer();
                timer1.Enabled          = true;
                timer1.Elapsed          += new System.Timers.ElapsedEventHandler(runService);
                timer1.Interval         = 1000 *interval;
                writeToLog("Starting Service");
                timer1.Start();

            }

            public void monitorServiceRun(){

                while(timer1.Enabled){
                    Thread.Sleep(100);
                }
             

            }
                
            protected override void OnStop(){
				  
                timer1.Enabled = false;
                writeToLog("Stopped "+ this.ServiceName+" Service at "+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss tt"));

            }

		    public  static  System.Data.DataTable  getDataFromStore(string theScript){
            System.Data.DataTable  dt = new DataTable();

				try{
						using (SQLiteConnection  liteConnect = new SQLiteConnection(liteConnectionString)){
							liteConnect.Open();
							SQLiteCommand cmd = new SQLiteCommand(theScript, liteConnect);
							cmd.CommandTimeout =0;
							SQLiteDataReader  reader = cmd.ExecuteReader();
							dt.Load(reader);	
							cmd.Dispose();
						}
				} catch(Exception e){
					
                 Console.WriteLine(theScript);
				 writeToLog(e.Message);
				 writeToLog(e.StackTrace);


				}
			return dt;
        }

    public static void executeOnDataStore(string sqlScript){

                        try{
                            using  (SQLiteConnection  liteConnect = new SQLiteConnection(liteConnectionString)){
                                                              
                                    liteConnect.Open();
                                    SQLiteCommand command = new SQLiteCommand(sqlScript, liteConnect);
                                    command.CommandTimeout = -1;
                                    command.ExecuteNonQuery();
                                    command.Dispose();
                                }
                                    
                                    
                        } catch(Exception e){
                        
                         writeToLog(e.Message);
                         writeToLog(e.StackTrace);

                            
                        }

                        
                    }

                public static void log(string logMessage){
                    
            }
                public static void  writeToLog(string logMessage){

                    executeOnDataStore(logInsertScript.Replace("log_time_val", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss tt")).Replace("log_message_val",logMessage));

            }
					
	public static void Main(string[] args){
		 
		 string servName     = "";
		 string servPath     = "";
		 string exe          = "";
		 
		 try {	
						for(int i =0; i< args.Length; i++){
							if(args[i].ToLower()=="-s" && !string.IsNullOrEmpty(args[(i+1)] ) && args[(i+1)].Length!=0){
								servName =  (args[(i+1)]);	
							}else if(args[i].ToLower()=="-e" && !string.IsNullOrEmpty(args[(i+1)])  && args[(i+1)].Length!=0){
								exe =  args[(i+1)];	
							}else if(args[i].ToLower()=="-p" && !string.IsNullOrEmpty(args[(i+1)]) && args[(i+1)].Length!=0){
								servPath =  args[(i+1)];	
							}
						}
                        initLiteConnectionString(exe);
                       
		 }catch(Exception e){

             Console.WriteLine(e.Message);
			 
		        writeToLog(e.Message);
				writeToLog(e.StackTrace);
               writeToLog("Error: "+e.ToString());
            
                var st = new StackTrace(e, true);

			 
		 }

		System.ServiceProcess.ServiceBase.Run(new TemplateServiceWrapper(servName,servPath, exe));
   
		 
	 }

     }

     
	  
}