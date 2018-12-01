
            using  System;
            using System.Collections;
            using System.Collections.Generic;
            using System.Text;
            namespace TranCopyService
            {

            public class TranCopyConfiguration  {

            public string    source_server                   	   { set; get;} 
            public string    source_database                       { set; get;} 
            public int       source_port                   		   { set; get;} 
            public string    destination_server                   { set; get;} 
            public string    destination_database                 { set; get;} 
            public int       destination_port                     { set; get;} 

            public string     staging_server                   { set; get;} 
            public string     staging_database                 { set; get;} 
            public int        staging_port                     { set; get;} 

            public string    destination_table                    {set; get;}
            public int        batch_size                           { set; get;} 
            public string    copy_table_name_prefix        { set; get;} 
            public string    copy_filter_table_prefix     { set; get;} 
            public string    copy_type                    { set; get;} 
            public int    copy_mode                     { set; get;} 
            public ArrayList    copy_specific_parameter_values  {set; get;}      
            public  string   copy_start_parameter {set; get;}

            public string    copy_end_parameter {set; get;}

            public string    copy_start_parameter_value {set; get;}

            public string   copy_end_parameter_value {set; get;}

            public string    copy_filter_script {set; get;}

            public string   copy_script {set; get;}
            public string    to_address                   		{ set; get;} 
            public string    from_address                  		{ set; get;} 
            public string    bcc_address                  		{ set; get;} 
            public string    cc_address                   		{ set; get;} 
            public string    smtp_server                   		{ set; get;} 
            public int       smtp_port                  		{ set; get;} 
            public string    sender                   		{ set; get;} 
            public string    sender_password                   	{ set; get;} 
            public bool      is_ssl_enabled                   	  {set; get;} 
            public string    alternate_row_colour                 { set; get;} 
            public string    email_font_family                  { set; get;} 
            public string    email_font_size                  	{ set; get;} 
            public string    color                 			{ set; get;} 
            public string    border_color                   		{ set; get;}  

            public string    border_width                   		{ set; get;}          
            public bool     send_email_notification              { set; get;}     
            public int       wait_interval                      { set; get;}
            public  int      num_of_previous_days_from_start         {set; get;}
            public  string   copy_filter_field               {set; get;}
            public  string   email_separator                         {set; get;}
            public  string   header_background_color                 {set; get;}
            public  bool     clean_up_after_copy                     {set; get;}
            public ArrayList destination_table_column_order         {set; get;}
            public bool      send_mail_on_error                      {set; get;}
            public  string  final_select_fields                    {set; get;}
             
             public string  filtered_staging_table_prefix         {set; get;}    

             public string  table_merge_script         {set; get;}  

             public bool  show_parameters_in_mail      {set; get;}  

             public string email_subject               {set; get;}

           
               }
            }  