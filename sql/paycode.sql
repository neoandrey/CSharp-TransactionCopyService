SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;


SELECT
post_tran_id, 
pp.post_tran_cust_id,
tran_nr,
pan , 
terminal_id, 
card_acceptor_id_code,
card_acceptor_name_loc, 
totals_group, 
tran_type, 
extended_tran_type, 
message_type, 
tran_amount_req, 
system_trace_audit_nr,
datetime_req, 
retrieval_reference_nr, 
tran_tran_fee_req , 
acquiring_inst_id_code,
rsp_code_rsp, 
terminal_owner, 
sink_node_name, 
merchant_type, 
source_node_name, 
from_account_id,
online_system_id,
settle_currency_code,
tran_currency_code,
pos_terminal_type,
settle_amount_impact,
settle_amount_rsp,
auth_id_rsp,
payee 

FROM   (  select * from post_tran t  with(nolock)  JOIN 
	
	(
	SELECT   rdate =[date] FROM dbo.get_dates_in_range(@StartDate, @StartDate)
	) r
       ON
       r.rdate =	 CONVERT(DATE, recon_business_date)

 and  extended_tran_type = '8234'
       and (LEFT(sink_node_name,2) <>'SB')
            and ( charindex('TPP', sink_node_name ) <1)
           AND (sink_node_name NOT IN ('GPRsnk', 'VTUsnk','SWTASPPOSsnk','ASPPOSIMCsnk','ASPPOSLMCsnk','ASPPOSVISsnk','ASPPOSVINsnk','SWTWEBABPsnk','SWTWEBGTBsnk','SWTWEBEBNsnk','SWTWEBUBAsnk')) 
            and settle_amount_impact != 0
   and  (tran_completed = 1) 
            AND (tran_reversed = 0) 
            AND (tran_type = ('00'))
			 AND (rsp_code_rsp = '00') 
            AND (message_type =('0200')) 
       AND (tran_postilion_originated = 0) 
       AND dbo.formatTranTypeStr(tran_type, extended_tran_type, message_type) !='Unknown'

) pp INNER JOIN
     dbo.post_tran_cust cc (NOLOCK) 
            ON pp.post_tran_cust_id = cc.post_tran_cust_id
           
	    AND (cc.terminal_id) ='4QTL0001' AND (cc.source_node_name ='SWTMOBILEsrc')
      OPTION (recompile, maxdop 2, optimize for unknown)