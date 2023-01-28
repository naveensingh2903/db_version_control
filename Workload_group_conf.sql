--Create/Alter workload group
-------------------------------------------------------------------------------------------
Create WORKLOAD GROUP wg_regulatory_report
 WITH
 (  MIN_PERCENTAGE_RESOURCE = 0,
	CAP_PERCENTAGE_RESOURCE = 39, 
	REQUEST_MIN_RESOURCE_GRANT_PERCENT = 3,  
	REQUEST_MAX_RESOURCE_GRANT_PERCENT = 3,  
	IMPORTANCE = ABOVE_NORMAL,  
	QUERY_EXECUTION_TIMEOUT_SEC = 3600  )          --1 hrs 
-------------------------------------------------------------------------------------------
Alter WORKLOAD GROUP wg_regulatory_report           --Alter the Workload Group at 9 AM based on ETL completion
 WITH
 (   MIN_PERCENTAGE_RESOURCE = 39
   , CAP_PERCENTAGE_RESOURCE = 39
   , REQUEST_MIN_RESOURCE_GRANT_PERCENT = 3
 ,  REQUEST_MAX_RESOURCE_GRANT_PERCENT = 3
  ,  IMPORTANCE =  ABOVE_NORMAL
   ,  QUERY_EXECUTION_TIMEOUT_SEC = 3600  )         --1 hrs 
-------------------------------------------------------------------------------------------
Alter WORKLOAD GROUP wg_regulatory_report           --Alter the Workload Group at 11 AM based on ETL completion
 WITH
 (   MIN_PERCENTAGE_RESOURCE = 0
   , CAP_PERCENTAGE_RESOURCE = 39
   , REQUEST_MIN_RESOURCE_GRANT_PERCENT = 3
 ,  REQUEST_MAX_RESOURCE_GRANT_PERCENT = 3
  ,  IMPORTANCE =  ABOVE_NORMAL
   ,  QUERY_EXECUTION_TIMEOUT_SEC = 3600  )         --1 hrs 
-------------------------------------------------------------------------------------------
--Create a new login (new_login) for regulatory report then assign all the roles which ibl_reporting user has.
--Create a new classifier and map the new_login user to new classifier.
-------------------------------------------------------------------------------------------
CREATE WORKLOAD CLASSIFIER wg_classifier_regulatory
  WITH (WORKLOAD_GROUP = 'wg_regulatory_report'
       ,MEMBERNAME = 'new_login'
      ,IMPORTANCE = above_normal);                  --This importance can override the importance defined in workload group.
-------------------------------------------------------------------------------------------
DROP WORKLOAD CLASSIFIER wg_classifier_regulatory;
-------------------------------------------------------------------------------------------
select * from sys.dm_pdw_workload_management_workload_groups


