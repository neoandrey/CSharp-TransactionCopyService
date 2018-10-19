SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

	SELECT	   c.source_node_name,
			t.datetime_tran_local,
			t.datetime_req,
			c.terminal_id,
			c.card_acceptor_name_loc, 
			tran_type_description, 
			t.retrieval_reference_nr, 	
			t.system_trace_audit_nr,		
			settle_amount_impact,
			settle_tran_fee,					
			currency_alpha_code,
			t.from_account_id,
			from_account_type,
			t.to_account_id,
			to_account_type,
			c.post_tran_cust_id,
			--c.source_node_name,
			t.sink_node_name,
			rsp_code_rsp,
			Response_Code_description,
			acquiring_inst_id_code,
			terminal_owner,
			payee
			
		INTO  #report_results 				
	FROM
		 (select 
		 
			datetime_tran_local,
			datetime_req,
		    tran_type_description =	dbo.formatTranTypeStr(tran_type, extended_tran_type, message_type) , 
			retrieval_reference_nr, 	
			system_trace_audit_nr,		
			dbo.formatAmount(
					CASE
						WHEN (tran_type = '51') THEN -1 * settle_amount_impact
						ELSE settle_amount_impact
					END, settle_currency_code) AS settle_amount_impact,
			dbo.formatAmount(settle_tran_fee_rsp, settle_currency_code) AS settle_tran_fee,					
			dbo.currencyAlphaCode(settle_currency_code) AS currency_alpha_code,
			from_account_id,
			dbo.rpt_fxn_account_type(from_account_type) AS from_account_type,
			to_account_id,
			dbo.rpt_fxn_account_type(to_account_type) AS to_account_type,
			post_tran_cust_id,
			sink_node_name,
			rsp_code_rsp,
			dbo.formatRspCodeStr(rsp_code_rsp) AS Response_Code_description,
			acquiring_inst_id_code,
			payee
	 
		  FROM	post_tran (NOLOCK, index(ix_post_tran_7))  WHERE  CONVERT(DATE,datetime_req) >=@StartDate and CONVERT(DATE,datetime_req)<=@EndDate
				 
		    AND tran_completed = 1
		    AND  (from_account_id = @fullpan or  to_account_id = @fullpan)
			AND   tran_postilion_originated = 0 
			AND	(message_type IN ('0200','0220','0420') )	
			AND	tran_type IN ('00', '01','02', '09', '20', '21', '40', '50' )
			AND (RIGHT(sink_node_name,5) = ('CCsnk')  or RIGHT(sink_node_name,6) =   'MPPsnk')
		
		) t		
		 	INNER JOIN 
		 (SELECT * FROM 	post_tran_cust  (NOLOCK, INDEX(PK_POST_TRAN_CUST))   WHERE 
		LEFT( pan,6) in (LEFT(@MaskedPAN,6), left(@fullpan,6)) AND 
		right( pan,4) in (right(@MaskedPAN,4), right(@fullpan,4))

			) c ON (t.post_tran_cust_id = c.post_tran_cust_id)
			OPTION (RECOMPILE, maxdop 1, OPTIMIZE FOR UNKNOWN)


		CREATE INDEX ix_report_result ON #report_results(retrieval_reference_nr,system_trace_audit_nr, from_account_id,to_account_id)
			
		SELECT * , bank_info=(SELECT  TOP 1 sink_node_name FROM postilion_office.dbo.post_tran with (NOLOCK) 
		WHERE t.retrieval_reference_nr = retrieval_reference_nr 
		      AND t.system_trace_audit_nr = system_trace_audit_nr
			   and t.from_account_id = from_account_id 
			   and t.to_account_id = to_account_id and source_node_name != 'AACardSrc' )  FROM #report_results t
			   OPTION (RECOMPILE, maxdop 1, OPTIMIZE FOR UNKNOWN)