
IF  (OBJECT_ID('tempdb.dbo.#PAN_TABLE') is null) begin
 
	CREATE TABLE #PAN_TABLE  (pan  VARCHAR(20)) 
	create index ix_pan ON   #PAN_TABLE (pan)
end
ELSE  BEGIN 
TRUNCATE TABLE #PAN_TABLE
INSERT INTO  #PAN_TABLE  VALUES

('506105*********3029'),('506100*********3009'),('539983******0186'),('539983******3314'),('532732******0181'),('539983******3063'),('539983******2253'),('539983******2613'),('539983******8614'),('539983******7886'),('539983******7886'),('539983******7886'),('539983******3567'),('539983******8738'),('539983******5511'),('532732******6790'),('539983******0307'),('539983******6148'),('539983******0894'),('539983******7886'),('539983******7886'),('539983******7886'),('539983******7886'),('539983******9290'),('539983******2332'),('539983******7886'),('539983******7886'),('539983******7886'),('539983******7886'),('539983******7886'),('539983******7116'),('539983******1265'),('539983******9434'),('539983******1465'),('539983******5159'),('539983******8925'),('539983******8925'),('539983******8925'),('539983******0701'),('532732******6478'),('539983******3980'),('532732******2568'),('539983******2655'),('539983******1067'),('539983******2422'),('532732******6429'),('539983******3003'),('539983******5771'),('539983******1032'),('539983******9067'),('539983******6593'),('532732******4966'),('539983******9598'),('539983******4811'),('539983******4811'),('539983******7092'),('539983******9598'),('539983******1261'),('539983******1757'),('539983******1954'),('539983******5771'),('532732******5479'),('532732******4856'),('539983******1507'),('539983******9598'),('539983******1096'),('539983******9486'),('539983******7738'),('539983******7975'),('539983******9487'),('539983******6379'),('532732******5411'),('540761******8702'),('539983******3914'),('532732******2766'),('539983******7642'),('539983******7146'),('539983******9487'),('539983******1506'),('539983******8454'),('539983******0142'),('539983******7162'),('539983******7975'),('539983******0142'),('539983******0142'),('539983******0142'),('539983******2081'),('539983******0142'),('539983******0142'),('539983******4458'),('532732******1054'),('532732******2244'),('532732******2244'),('539983******0142'),('539983******8133'),('532732******3117'),('532732******4073'),('539983******8287'),('539983******7421'),('539983******7642'),('539983******9413'),('539983******4098'),('539983******7146'),('539983******9424'),('532732******5800'),('539983******3481'),('539983******8464'),('539983******5759'),('532732******3200'),('540761******1268'),('532732******1105'),('539983******2714'),('539983******5934'),('539983******5934'),('532732******2524'),('539983******8757'),('539983******5539'),('539983******5497'),('539983******6641'),('539983******6641'),('539983******4207'),('539983******4254'),('539983******4940'),('539983******3329'),('539983******2441'),('539983******5934'),('539983******9859'),('532732******2479'),('539983******1272'),('539983******7210'),('539983******2249'),('539983******0006'),('532732******4285'),('539983******0448'),('539983******0448'),('532732******6045'),('539983******1838'),('532732******1760'),('539983******0485'),('532732******4285'),('540761******6618'),('532732******7649'),('532732******3566'),('539983******8943'),('539983******4928'),('540761******6618'),('539983******4638'),('539983******9626'),('532732******6688'),('539983******6094'),('539983******2713'),('539983******1622'),('539983******1622'),('539983******2881'),('539983******5274'),('539983******8521'),('539983******5858'),('539983******4858'),('539983******2887'),('539983******7532'),('539983******7210'),('539983******4966'),('539983******8541'),('539983******4537'),('539983******4933'),('532732******9422'),('539983******0006'),('532732******7299'),('539983******4638'),('539983******6094'),('539983******6094'),('539983******9424'),('539983******9424'),('539983******5727'),('539983******8055'),('532732******6966'),('539983******2604'),('539983******6249'),('539983******7825'),('539983******7825'),('539983******5063'),('539983******6996'),('539983******3696'),('532732******7092'),('539983******4549'),('539983******0974'),('539983******1414'),('539983******8802'),('539983******5491'),('539983******5354'),('539983******7935'),('539983******2160'),('539983******9410'),('539983******8802'),('539983******0986'),('539983******4151'),('539983******4426'),('539983******6172'),('539983******6172'),('539983******6172'),('539983******0619'),('539983******6172'),('539983******5820'),('420320******0424'),('539983******4930'),('539983******8843'),('539983******0632'),('539983******9258'),('539941******3058')
END

IF  (OBJECT_ID('tempdb.dbo.#TERMINAL_TABLE') is  null) begin
		CREATE TABLE #TERMINAL_TABLE  (terminal_id  VARCHAR(10)) 
		create index ix_terminal_id ON   #TERMINAL_TABLE (terminal_id)
END
ELSE BEGIN

INSERT INTO  #TERMINAL_TABLE

VALUES ('2058E507'),('2058EH74'),('20584559'),('20584559'),('20584559'),('20584559'),('20584559'),('2058CR76'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058CR77'),('2058E503'),('2058E503'),('2058E503'),('2058E505'),('2058E505'),('2058E505'),('2058E505'),('2058E505'),('2058E505'),('2058E507'),('2058E507'),('2058E507'),('2058E507'),('2058E507'),('2058E507'),('2058E507'),('2058E508'),('2058E508'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058E509'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH72'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058EH73'),('2058T177'),('2058T177'),('2058T177'),('2058T177'),('2058T177'),('2058T177'),('2058T177'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U876'),('2058U877'),('2058U877'),('2058U877'),('2058U877'),('2058U877'),('2058U879'),('2058U879'),('2058U879'),('2058U879'),('2058U879'),('3nqt0001'),('3nqt0001'),('3nqt0001'),('3PWQ0001'),('3PWQ0001'),('3PWQ0001'),('2058E509')
END


SELECT  *,CASE 
WHEN structured_data_req is null or LEN(convert(varchar(max),structured_data_req))=0 THEN null
when  convert(varchar(max),structured_data_req) like   '%paymentReference>%'  then
SUBSTRING(structured_data_req, CHARINDEX('paymentReference>',structured_data_req)+17, (CHARINDEX('</paymentReference>',structured_data_req)-CHARINDEX('paymentReference>',structured_data_req)-17)) 
else ''
END as paymentReference, 
clear_pan= dbo.usf_decrypt_pan(pan,pan_encrypted)FROM  (
select b.pan, pan_encrypted, message_type,tran_type,source_node_name ,tran_amount_req,sink_node_name,datetime_req,retrieval_reference_nr,system_trace_audit_nr,rsp_code_rsp,b.terminal_id, structured_data_req
FROM  (   SELECT post_tran_cust_id, message_type, tran_type,tran_amount_req,sink_node_name,datetime_req,retrieval_reference_nr,system_trace_audit_nr,rsp_code_rsp, structured_data_req FROM  POST_TRAN pt (NOLOCK, index=ix_post_tran_7)
  JOIN
   ( SELECT [date] FROM dbo.get_dates_in_range(@StartDate,@EndDate) )r
   ON CONVERT(DATE, pt.datetime_req) = r.[Date]
   

   )a
 LEFT JOIN POST_TRAN_CUST b (NOLOCK) 
ON a.post_tran_cust_id = b.post_tran_cust_id 
LEFT JOIN  #PAN_TABLE p
ON  p.pan = b.pan
LEFT  JOIN  #TERMINAL_TABLE t 
ON t.terminal_id = b.terminal_id
) final
OPTION (RECOMPILE ,  MAXDOP 3, OPTIMIZE FOR UNKNOWN)