-----------------------------------------------------------------
--Main tables 
select count_big(*) from [edw].[IBL0784_SLE_MIS_TBL];	65743492
select count_big(*) from edw.factbaldly ;				40837025883
select count_big(*) from edw.dimaccount;				197310881
select count_big(*) from edw.dimcustomer ;				97089391
select count_big(*) from edw.dimserviceoutlet ;			15771
select count_big(*) from edw.DimCode ;					55394
select count_big(*) from Edw.Fact_Loan_Schedule_Risk;	22349971
select count_big(*) from Edw.Fact_Loan_Overdue_OS_Risk;	297477839
select count_big(*) from CASA.DIMBRANCH;				1882
select count_big(*) from EDW.DimGLPL;					1275
select count_big(*) from edw.facttran_nbk_mbk;			322710306
select count_big(*) from EDW.FASTAG_TOLL;				206371190
select count_big(*) from EDW.FACTATMTRANSACTIONs;		961249665
select count_big(*) from EDW.FACTTRANSACTIONDLY;		10659142721
select count_big(*) from EDW.DIMTRANSACTIONTYPE ;		14535
select count_big(*) from EDW.DIMREGISTRATION_NBK_MBK;	473594834	
select count_big(*) from EDW.[FACTTRAN_UPI];			2451762525
select count_big(*) from EDW.[DIMOPTINUSERS_WA];		5200007
select count_big(*) from edw.FactPOSTransactions;		147093254
-----------------------------------------------------------------
--staging tables  
select count_big(*) from edwStg.stgIBL0784_SLE_MIS_TBL;
select count_big(*) from edwStg.stgsol;
select count_big(*) from edwStg.stgDIM_CODE;
select count_big(*) from edwStg.stgIBL0808_C_LARSH_TBL;
select count_big(*) from edwStg.stgIBL0808_LAA_DAILY_OS_TBL;
select count_big(*) from edwStg.stgPMTS;
select count_big(*) from edwStg.stgBTDT;
select count_big(*) from edwStg.stgrqdt;
select count_big(*) from edwStg.stgREQUEST_TABLE;
select count_big(*) from edwStg.stgCUSR;
select count_big(*) from edwStg.stgBRPF;
select count_big(*) from edwStg.stgabrt;
select count_big(*) from edwStg.stgIMPS_FT_DETAIL;
select count_big(*) from edwStg.stgbankingcif;
select count_big(*) from edwStg.stgusername;
select count_big(*) from edwStg.stgcreditcif;
select count_big(*) from edwStg.stgcustomername;
select count_big(*) from edwStg.stgmobilenumber;
select count_big(*) from edwStg.stgemailid;
select count_big(*) from edwStg.stgdob;
select count_big(*) from edwStg.stgstatusflag;
select count_big(*) from edwStg.stglastlogindate;
select count_big(*) from edwStg.stgregistrationdate;
select count_big(*) from edwStg.stgdeviceosversion;
select count_big(*) from edwStg.stgdevicemodel;
select count_big(*) from edwStg.stgprimarycardno;
select count_big(*) from edwStg.stgVIRTUALADDRESS;
select count_big(*) from edwStg.stgregistrationmode;
select count_big(*) from edwStg.stgprimarydeviceid;

select count_big(*) from edwStg.StgACCOUNTS;
select count_big(*) from edwStg.StgACD;
select count_big(*) from edwStg.StgACTIVITIES;
select count_big(*) from edwStg.StgADDRESS;
select count_big(*) from edwStg.Stgc_cam;
select count_big(*) from edwStg.Stgc_consumer_morat;
select count_big(*) from edwStg.Stgc_crr;
select count_big(*) from edwStg.StgC_DLRY;
select count_big(*) from dbo.StgC_GAC;
select count_big(*) from edwStg.StgC_GAC;
select count_big(*) from edwStg.StgC_GAM;
select count_big(*) from edwStg.Stgc_irt;
select count_big(*) from edwStg.StgC_LAM;
select count_big(*) from edwStg.StgC_LLT;
select count_big(*) from edwStg.Stgc_non_consumer_morat;
select count_big(*) from edwStg.StgC_SBGAM;
select count_big(*) from dbo.StgC_TKL;
select count_big(*) from edwStg.StgC_TKL;
select count_big(*) from CASA.StgC_TKL;
select count_big(*) from edwStg.StgCAM;
select count_big(*) from edwStg.StgCATEGORIES;
select count_big(*) from edwStg.Stgcategory_lang;
select count_big(*) from edwStg.Stgcorporate;
select count_big(*) from edwStg.StgCOT;
select count_big(*) from edwStg.Stgcreditrating;
select count_big(*) from edwStg.StgDEMOGRAPHIC;
select count_big(*) from edwStg.StgDHT;
select count_big(*) from edwStg.StgDIM_ACCT_AGGREGATE;
select count_big(*) from edwStg.StgEAB;
select count_big(*) from edwStg.StgEIT;
select count_big(*) from edwStg.Stgentitydocument;
select count_big(*) from edwStg.StgFD_QUICK;
select count_big(*) from edwStg.StgFIN_CUSTOMER_ADDRESS_PIVOT;
select count_big(*) from edwStg.StgFINANCIAL;
select count_big(*) from edwStg.StgFINANCIALDETAILS;
select count_big(*) from edwStg.StgFinancialTransaction;
select count_big(*) from edwStg.StgGAC;
select count_big(*) from edwStg.StgGAM;
select count_big(*) from edwStg.StgGEM;
select count_big(*) from edwStg.StgGROUPHOUSEHOLD;
select count_big(*) from edwStg.StgGROUPING;
select count_big(*) from edwStg.StgHOUSEHOLD;
select count_big(*) from edwStg.StgIBL_REVIEWEXT_TBL;
select count_big(*) from edwStg.StgIBL0475_C_INST_ACCT_MOD_TBL;
select count_big(*) from edwStg.StgIBL0553_C_SB_FD_Acct_OPN_TBL;
select count_big(*) from edwStg.Stgibl0637_new_dpd_master_tbl;
select count_big(*) from edwStg.Stgibl0802_cust_loan_reschld_tbl;
select count_big(*) from edwStg.StgINTEREST_RATE;
select count_big(*) from edwStg.StgITC;
select count_big(*) from edwStg.StgITCPNL;
select count_big(*) from edwStg.StgLAC_CLT;
select count_big(*) from dbo.Stglam;
select count_big(*) from edwStg.StgLAM;
select count_big(*) from edwStg.StgLHT;
select count_big(*) from edwStg.StgLLT;
select count_big(*) from edwStg.StgMISCELLANEOUSINFO;
select count_big(*) from edwStg.StgNonFinancialTransaction;
select count_big(*) from edwStg.StgPaymentHistory;
select count_big(*) from edwStg.StgRENO;
select count_big(*) from edwStg.StgRETBASEL;
select count_big(*) from edwStg.StgRTL;
select count_big(*) from edwStg.StgSMT;
select count_big(*) from edwStg.StgTA_COT;
select count_big(*) from edwStg.StgTAM;
select count_big(*) from edwStg.StgTERM_DEPOSIT_OVERDUE_GL_CODE;
select count_big(*) from edwStg.StgVTMESTR;
select count_big(*) from edwStg.StgVTUTMETRNREQ;
select count_big(*) from edwStg.StgVTUTTRNREQ;
select count_big(*) from edwStg.Stgvtuttrnreqacc;
select count_big(*) from edwStg.StgVTUTTRNREQPY;
----------------------------------------------------------------
--staging2 tables 
select count_big(*) from edwStg.stg2Accounts;
select count_big(*) from edwStg.stg2ACD;
select count_big(*) from edwStg.stg2C_CAM;
select count_big(*) from edwStg.stg2C_CRR;
select count_big(*) from edwStg.Stg2C_DLRY;
select count_big(*) from edwStg.stg2C_GAC;
select count_big(*) from edwStg.Stg2C_IRT;
select count_big(*) from edwStg.stg2C_LAM;
select count_big(*) from edwStg.stg2C_SBGAM;
select count_big(*) from edwStg.stg2CAM;
select count_big(*) from edwStg.stg2CORPORATE;
select count_big(*) from edwStg.STG2COT;
select count_big(*) from edwStg.Stg2DHT;
select count_big(*) from edwStg.STG2EIT;
select count_big(*) from edwStg.stg2EntityDocument;
select count_big(*) from edwStg.Stg2FD_QUICK;
select count_big(*) from edwStg.stg2GAC;
select count_big(*) from edwStg.Stg2GAM;
select count_big(*) from edwStg.stg2GEM;
select count_big(*) from edwStg.stg2Grouping;
select count_big(*) from edwStg.stg2HouseHold;
select count_big(*) from edwStg.Stg2ITC;
select count_big(*) from edwStg.stg2ITCPNL;
select count_big(*) from edwStg.Stg2lac_clt;
select count_big(*) from edwStg.stg2LAM;
select count_big(*) from edwStg.stg2LHT;
select count_big(*) from edwStg.stg2LLT;
select count_big(*) from edwStg.STG2RENO;
select count_big(*) from edwStg.stg2SMT;
select count_big(*) from edwStg.Stg2TA_COT;
select count_big(*) from edwStg.stg2TAM;

