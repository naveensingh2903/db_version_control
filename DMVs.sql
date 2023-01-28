-----------------------------------------------------------------------------------------------------------------
--TO GET CLUSTER UTILIZATION, TOTAL QUERIES, RUNNING QUERIES AND QUEUED QUERIES ON CLUSTER
SELECT SUM(COALESCE(resource_allocation_percentage,0)) as cluster_utilization, 
	count(*) as total_queries,
	SUM(CASE WHEN start_time IS NOT NULL THEN 1 ELSE 0 END) as running_queries, 
	SUM(CASE WHEN start_time IS NULL THEN 1 ELSE 0 END) as queued_queries
FROM sys.dm_pdw_exec_requests 
where status not in ('completed','cancelled','failed') and resource_class is not null
order by 2 desc
-----------------------------------------------------------------------------------------------------------------
--TO GET EACH RESOURCE_CLASS CURRENT AND MAX RUNNABLE QUERIES
select a.name,a.importance,cast(a.cap_percentage_resource/a.request_min_resource_grant_percent as int) max_queries,
b.queries_run,a.cap_percentage_resource,b.total_resource_consumption,cast(((b.total_resource_consumption/a.cap_percentage_resource)*100) as int) efficiency
from sys.workload_management_workload_groups a
left join datasturdy.view_resource b on a.name=b.resource_class
--where a.group_id>12
order by 6 desc
-----------------------------------------------------------------------------------------------------------------
--TO GET EACH RESOURCE_CLASS CURRENT AND MAX RUNNABLE QUERIES
select a.name,a.importance,(case when a.group_id>12 then cast(a.cap_percentage_resource/a.request_min_resource_grant_percent as int) 
when name='staticrc10' then 64 when name='staticrc20' then 60 
when name='staticrc20' then 60
when name='staticrc30' then 30
when name='staticrc40' then 15
when name='staticrc50' then 7
when name='staticrc60' then 3
when name='staticrc70' then 1
when name='staticrc80' then 1
when name='smallrc' then 32
when name='mediumrc' then 10
when name='largerc' then 4
when name='xlargerc' then 1
end )max_queries,
b.queries_run,a.cap_percentage_resource,b.total_resource_consumption,cast(((b.total_resource_consumption/a.cap_percentage_resource)*100) as int) efficiency
from sys.workload_management_workload_groups a
left join datasturdy.view_resource b on a.name=b.resource_class
--where a.group_id>12
order by 6 desc
-----------------------------------------------------------------------------------------------------------------
--DMV to see what all logins are running what all queries
select login_name,count(*) queries_running,sum(resource_allocation_percentage) total_resource_allocated from 
(
select datediff(mi,start_time,getdate()) mins_running,login_name,resource_allocation_percentage from sys.dm_pdw_exec_sessions s join sys.dm_pdw_exec_requests r
on s.session_id=r.session_id where r.status='running' and r.resource_class is not null
) s
group by login_name
order by 3 desc
-----------------------------------------------------------------------------------------------------------------
--DMV TO SEE NUMBER OF QUERIES WHICH ARE IN QUEUE
select distinct(rw.request_id)
from sys.dm_workload_management_workload_groups_stats s
join sys.dm_pdw_exec_requests r on r.group_name = s.name collate SQL_Latin1_General_CP1_CI_AS
join sys.dm_pdw_resource_waits rw on rw.request_id = r.request_id
where rw.State='Queued'
-----------------------------------------------------------------------------------------------------------------
--TO GET WHAT ALL QUERIES ARE IN WAITING STATE AND WHO IS THE OWNER OF THE QUERY
select * from sys.dm_pdw_exec_sessions
where request_id in (select distinct request_id from sys.dm_pdw_resource_waits where state='queued')
-----------------------------------------------------------------------------------------------------------------
--TO GET COUNT OF ALL THE QUERIES WHICH ARE IN QUEUE WITH CORRESPONDING LOGIN NAME
select login_name,count(*) from sys.dm_pdw_exec_sessions
where request_id in (select distinct request_id from sys.dm_pdw_resource_waits where state='queued')
group by login_name
-----------------------------------------------------------------------------------------------------------------
--TO GET THE COUNT OF ALL THE QUERIES WHICH ARE IN QUEUE WITH CORRESPONDING RESOURCE CLASS
select resource_class,count(*) from sys.dm_pdw_exec_requests
where request_id in (select distinct request_id from sys.dm_pdw_resource_waits where state='queued')
group by resource_class
-----------------------------------------------------------------------------------------------------------------
--TO GET WHY THE QUERIES ARE WAITING
select * from sys.dm_pdw_resource_waits
where request_id in (select distinct request_id from sys.dm_pdw_resource_waits where state='queued')
-----------------------------------------------------------------------------------------------------------------
--To get holistic overview of how many queries are waiting for which type of resource
select type,count(*) from sys.dm_pdw_resource_waits
where request_id in (select distinct request_id from sys.dm_pdw_resource_waits where state='queued')
group by type
order by 2 desc
-----------------------------------------------------------------------------------------------------------------
--To get how much time the queries waited before execution
select datediff(mi,submit_time,start_time) wait_time,* from sys.dm_pdw_exec_requests
where status='running' and resource_class is not null
order by 1 desc
-----------------------------------------------------------------------------------------------------------------
--To get suspended queries wait time 
select datediff(mi,submit_time,getdate()) wait_time,* from sys.dm_pdw_exec_requests
where status='suspended'
order by 1 desc
-----------------------------------------------------------------------------------------------------------------
--Check query is hitting result set caching or not
select * from sys.dm_pdw_request_steps 
where operation_type='ReturnOperation' and command like '%DWResultCacheDb%' and status='Running'
-----------------------------------------------------------------------------------------------------------------
--DMV TO SEE NUMBER OF QUERIES RUNNING ON EACH NODE
SELECT pdw_node_id, count( distinct request_id)
FROM sys.dm_pdw_sql_requests
WHERE status ='running'
group by pdw_node_id
order by 2 DESC
-----------------------------------------------------------------------------------------------------------------
--DMV TO SEE TOP 20 QUERIES WHICH ARE TAKING LONGEST TIME
select top 20 * from sys.dm_pdw_exec_requests
where [status]='Running'
order by total_elapsed_time DESC
-----------------------------------------------------------------------------------------------------------------
select top 50 * from sys.dm_pdw_exec_requests
where status='running' and resource_class is not null
order by total_elapsed_time desc

  
select * from sys.dm_pdw_exec_requests where session_id in (select distinct session_id from sys.dm_pdw_exec_sessions
where login_name='ibl_reporting' 
) order by 4 desc

select distinct request_id, submit_time from sys.dm_pdw_exec_Requests
where resource_class='staticrc40'
-----------------------------------------------------------------------------------------------------------------
--Queries running from which login 
select datediff(mi,start_time,getdate()),login_name,r.* from sys.dm_pdw_exec_sessions s join sys.dm_pdw_exec_requests r
on s.session_id=r.session_id where r.status='running' and r.resource_class is not null
order by total_elapsed_time desc
-----------------------------------------------------------------------------------------------------------------
--DMV TO SEE ALL THE QUERIES WHICH ARE EXECUTED FROM SAME SESSION_ID
select * from sys.dm_pdw_exec_requests
where request_id='QID391464231'
-----------------------------------------------------------------------------------------------------------------
select * from sys.dm_pdw_exec_requests
where session_id='SID21465957'
-----------------------------------------------------------------------------------------------------------------
--DMV TO SEE BREAKDOWN OF THE QUERY
select * from sys.dm_pdw_request_steps
where request_id='QID276412486'
-----------------------------------------------------------------------------------------------------------------
select * from sys.dm_pdw_sql_requests
where request_id = 'QID275951902'
and  step_index = '19'
-----------------------------------------------------------------------------------------------------------------
select pdw_node_id,count(*) from sys.dm_pdw_sql_requests
where status ='running' 
group by pdw_node_id 
order by 2 desc
-----------------------------------------------------------------------------------------------------------------
select distribution_id,count(*) from sys.dm_pdw_sql_requests
where status='running'
group by distribution_id
order by 2 desc
-----------------------------------------------------------------------------------------------------------------
select * from sys.dm_pdw_sql_requests
where pdw_node_id='3' and status='running'
order by total_elapsed_time desc
-----------------------------------------------------------------------------------------------------------------
select pdw_node_id,count(*) from sys.dm_pdw_sql_requests
where request_id='QID276623330' and step_index='2'
group by pdw_node_id
-----------------------------------------------------------------------------------------------------------------
select * from sys.dm_pdw_sql_requests
where pdw_node_id='3' and status='running'
order by total_elapsed_time desc
-----------------------------------------------------------------------------------------------------------------
--see how many running operations are going on each distribution
select distribution_id,count(*) from sys.dm_pdw_sql_requests
where status='running'
group by distribution_id
order by count(*) desc
-----------------------------------------------------------------------------------------------------------------
SELECT * FROM sys.dm_pdw_dms_workers
WHERE request_id = 'QID275951902' AND step_index = 14;
-----------------------------------------------------------------------------------------------------------------
select type,sum(bytes_processed) total_byte_processed,sum(rows_processed) total_rows_processed from sys.dm_pdw_dms_workers
where request_id = 'QID362491266' AND step_index ='2'
group by type
-----------------------------------------------------------------------------------------------------------------
select distribution_id,type,sum(bytes_processed) total_byte_processed,sum(rows_processed) total_rows_processed from sys.dm_pdw_dms_workers
where request_id = 'QID362491266' AND step_index ='2'
group by distribution_id,type
-----------------------------------------------------------------------------------------------------------------
SELECT * FROM sys.dm_pdw_lock_waits w
JOIN sys.dm_pdw_exec_requests r ON w.request_id = r.request_id
where r.session_id='SID676705346'
-----------------------------------------------------------------------------------------------------------------
--To get errors in ADW
select * from sys.dm_pdw_errors
where year(create_time)=2022 and month(create_time)=05 and day(create_time)=27
order by create_time desc
-----------------------------------------------------------------------------------------------------------------
--To check for any locks on a table
select * from sys.dm_pdw_lock_waits 
where object_name like '%unicommerce_myntra_grn%'

select  * from sys.dm_pdw_waits
where object_name like '%unicommerce_myntra_grn%'
-----------------------------------------------------------------------------------------------------------------
select request_id, session_id, submit_time, start_time, end_compile_time, end_time, command from sys.dm_pdw_exec_requests
where session_id = 'SID503146481'
-----------------------------------------------------------------------------------------------------------------
select request_id,session_id,total_elapsed_time,command,resource_class,importance,group_name,classifier_name,resource_allocation_percentage
from sys.dm_pdw_exec_requests
where [status] in ('Running','suspended') and resource_class is not NULL
order by total_elapsed_time DESC
-----------------------------------------------------------------------------------------------------------------
select sum(resource_allocation_percentage) as total_resource_consumption
from sys.dm_pdw_exec_requests
where [status] in ('Running','suspended') and resource_class is not NULL

select resource_class,count(*), sum(resource_allocation_percentage) as total_resource_consumption from sys.dm_pdw_exec_requests
where status='running' and resource_class is not null
group by resource_class

select name,importance,cast(cap_percentage_resource/request_min_resource_grant_percent as int) max_queries,cap_percentage_resource
from sys.workload_management_workload_groups
where group_id>12

--Create the view 'viewresource'
create view view_resource as
select resource_class,count(*) queries_run,sum(resource_allocation_percentage) as total_resource_consumption from sys.dm_pdw_exec_requests
where status='running' and resource_class is not null
group by resource_class
-----------------------------------------------------------------------------------------------------------------
--To know the number of open sessions.
select count(*) from sys.dm_pdw_exec_sessions
where status in ('idle','active')
-----------------------------------------------------------------------------------------------------------------
exec sp_kill_idle_session_30min
-----------------------------------------------------------------------------------------------------------------
--To see data source and file formats use these queries:
select * from sys.external_file_formats where name='DDPCommaDelimitedFile_SkipHeader'
select * from sys.external_data_sources where name='adw-commerce'
-----------------------------------------------------------------------------------------------------------------
--Operation_type
/*
DMS query plan operations: 'ReturnOperation', 'PartitionMoveOperation', 'MoveOperation', 'BroadcastMoveOperation', 'ShuffleMoveOperation', 'TrimMoveOperation', 'CopyOperation', 'DistributeReplicatedTableMoveOperation'

SQL query plan operations: 'OnOperation', 'RemoteOperation'

Other query plan operations: 'MetaDataCreateOperation', 'RandomIDOperation'
MetaDataCreateOperation=Stores data into or retrieves data from a system view. For example, this operation is used to perform a SELECT from a system view.

External operations for reads: 'HadoopShuffleOperation', 'HadoopRoundRobinOperation', 'HadoopBroadcastOperation'

External operations for MapReduce: 'HadoopJobOperation', 'HdfsDeleteOperation'

External operations for writes: 'ExternalExportDistributedOperation', 'ExternalExportReplicatedOperation', 'ExternalExportControlOperation'
*/
-----------------------------------------------------------------------------------------------------------------
--This gives total number of ‘idle’ and ‘active’ sessions
select count(*) from sys.dm_pdw_exec_sessions
where status<>'closed'
-----------------------------------------------------------------------------------------------------------------
--This gives total number of idle sessions as per login_name
select  login_name,count(*) from sys.dm_pdw_exec_sessions
where status='idle'
group by login_name
order by 2 DESC
-----------------------------------------------------------------------------------------------------------------
--To see relation of database ROLE and user.
SELECT DP1.name AS DatabaseRoleName,
isnull (DP2.name, 'No members') AS DatabaseUserName
FROM sys.database_role_members AS DRM
RIGHT OUTER JOIN sys.database_principals AS DP1
ON DRM.role_principal_id = DP1.principal_id
LEFT OUTER JOIN sys.database_principals AS DP2
ON DRM.member_principal_id = DP2.principal_id
WHERE DP1.type = 'R'
--and DP2.name in ('adhoc_myntra_fin_user', 'adhoc_myntra_clickstream_fin')
ORDER BY DP1.name;
-----------------------------------------------------------------------------------------------------------------
--To see workload group, classifier details
SELECT wg.group_id, wg.name as group_name, wc.name as classifer_name, cd.classifier_type, classifier_value as user_mapped, wg.importance, wc.importance
min_percentage_resource, cap_percentage_resource, request_min_resource_grant_percent, query_execution_timeout_sec
FROM sys.workload_management_workload_groups wg join sys.workload_management_workload_classifiers wc
join sys.workload_management_workload_classifier_details cd on cd.classifier_id = wc.classifier_id
on wg.name = wc.group_name order by wg.name
-----------------------------------------------------------------------------------------------------------------
--To see what all users are mapped to a resource group along with classifier names
SELECT wg.group_id, wg.name as group_name, wc.name as classifer_name, cd.classifier_type, classifier_value as user_mapped, wg.importance, wc.importance
min_percentage_resource, cap_percentage_resource, request_min_resource_grant_percent, query_execution_timeout_sec
FROM sys.workload_management_workload_groups wg join sys.workload_management_workload_classifiers wc
join sys.workload_management_workload_classifier_details cd on cd.classifier_id = wc.classifier_id
on wg.name = wc.group_name 
where wg.name like '%wg_hflr_myntrapoc%'
-----------------------------------------------------------------------------------------------------------------
--To see user belongs to which particular resource group
SELECT wg.group_id, wg.name as group_name, wc.name as classifer_name, cd.classifier_type, classifier_value as user_mapped, wg.importance, wc.importance
min_percentage_resource, cap_percentage_resource, request_min_resource_grant_percent, query_execution_timeout_sec
FROM sys.workload_management_workload_groups wg join sys.workload_management_workload_classifiers wc
join sys.workload_management_workload_classifier_details cd on cd.classifier_id = wc.classifier_id
on wg.name = wc.group_name 
where classifier_value like '%hflr_fast_lane%'
-----------------------------------------------------------------------------------------------------------------
--DMVs for user, wg and classifiers
select * from sys.workload_management_workload_groups
select * from sys.workload_management_workload_classifiers
select * from sys.workload_management_workload_classifier_details
select * from sys.database_role_members
select * from sys.database_principals
select * from sys.sysusers
-----------------------------------------------------------------------------------------------------------------
--This DMV will give user and its permission
select  princ.name,princ.type_desc,perm.permission_name,perm.state_desc,perm.class_desc,object_name(perm.major_id)
from
sys.database_principals princ
left join sys.database_permissions perm
on perm.grantee_principal_id = princ.principal_id
where princ.name = '[USERNAME IN DW]'
-----------------------------------------------------------------------------------------------------------------
select * from sys.dm_pdw_exec_requests r
join sys.dm_pdw_exec_sessions s
on s.session_id=r.session_id
where s.[status]='active' and r.status='running' and s.login_name='foci_admin'
-----------------------------------------------------------------------------------------------------------------
SELECT total_elapsed_time / 60000 run_min,
DATEDIFF(minute,start_time,getdate ()),
a.*
FROM sys.dm_pdw_exec_requests a
WHERE status NOT IN ('Completed','Failed','Cancelled')
AND   session_id <> session_id()
--AND   resource_class IS NOT NULL
AND   lower(command) LIKE '%fact_core_item%'
ORDER BY submit_time DESC;
-----------------------------------------------------------------------------------------------------------------
--This DMV will give user and its permission
select  princ.name,princ.type_desc,perm.permission_name,perm.state_desc,perm.class_desc,object_name(perm.major_id)
from
sys.database_principals princ
left join sys.database_permissions perm
on perm.grantee_principal_id = princ.principal_id
where princ.name = '[USERNAME IN DW]'
-----------------------------------------------------------------------------------------------------------------
--To see workload group, classifier details
SELECT wg.group_id, wg.name as group_name, wc.name as classifer_name, cd.classifier_type, classifier_value as user_mapped, wg.importance, wc.importance
min_percentage_resource, cap_percentage_resource, request_min_resource_grant_percent, query_execution_timeout_sec
FROM sys.workload_management_workload_groups wg join sys.workload_management_workload_classifiers wc
join sys.workload_management_workload_classifier_details cd on cd.classifier_id = wc.classifier_id
on wg.name = wc.group_name order by wg.name
-----------------------------------------------------------------------------------------------------------------
--user and their roles
SELECT DP1.name AS DatabaseRoleName,
isnull (DP2.name, 'No members') AS DatabaseUserName
FROM sys.database_role_members AS DRM
RIGHT OUTER JOIN sys.database_principals AS DP1
ON DRM.role_principal_id = DP1.principal_id
LEFT OUTER JOIN sys.database_principals AS DP2
ON DRM.member_principal_id = DP2.principal_id
WHERE DP1.type = 'R'
--and DP2.name in ('adhoc_myntra_fin_user', 'adhoc_myntra_clickstream_fin')
ORDER BY DP1.name;
----------------------------------------------------------------------------------------------------------------
SELECT distinct pdw_node_id, count(*) FROM sys.dm_pdw_sql_requests
WHERE [status]='Running' GROUP BY pdw_node_id --ORDER BY 3 desc;
-----------------------------------------------------------------------------------------------------------------
SELECT (total_elapsed_time/1000)/60 as time_in_min,*
FROM sys.dm_pdw_exec_requests
WHERE status not in ('Completed','Failed','Cancelled')
--and command like '%exec [dbo].[unicommerce_myntra_item_barcodes]%'
AND session_id <> session_id() and resource_class is not null order by total_elapsed_time desc
-----------------------------------------------------------------------------------------------------------------
select dateadd(minute,330,start_time) as starttime_IST,(total_elapsed_time/60000) as waiting_in_min,*
from sys.dm_pdw_request_steps where request_id='QID690553034'
-----------------------------------------------------------------------------------------------------------------
select * from sys.dm_pdw_sql_requests where request_id = 'QID690553034' and step_index = 20 and status <> 'complete'
-----------------------------------------------------------------------------------------------------------------
select * from sys.dm_pdw_nodes_exec_requests where pdw_node_id = 30 and session_id in (231)
-----------------------------------------------------------------------------------------------------------------
--To view table and its distribution:
select * from sys.pdw_table_distribution_properties p
join sys.tables t
on t.object_id = p.object_id
where t.object_id=object_id('[bidb].[fact_core_item]')

select * from sys.columns
where object_id=object_id('[bidb].[fact_core_item]')
-----------------------------------------------------------------------------------------------------------------
--TO VIEW TABLE AND ITS DISTRIBUTION COLUMN
select p.object_id,p.distribution_policy,p.distribution_policy_desc,t.max_column_id_used,z.name column_name,s.column_id,s.distribution_ordinal
from sys.pdw_table_distribution_properties p
join sys.tables t
on t.object_id = p.object_id
join sys.pdw_column_distribution_properties s
on p.object_id=s.object_id
join sys.columns z
on s.object_id=z.object_id and s.column_id=z.column_id
where t.object_id='2079722320'
and s.distribution_ordinal = 1
-----------------------------------------------------------------------------------------------------------------
--TO GET COUNT OF HASH TABLES 
select count(distinct p.object_id)
from sys.pdw_table_distribution_properties p
join sys.tables t
on t.object_id = p.object_id
join sys.pdw_column_distribution_properties s
on p.object_id=s.object_id
join sys.columns z
on s.object_id=z.object_id and s.column_id=z.column_id
where s.distribution_ordinal = 1
-----------------------------------------------------------------------------------------------------------------
--To see the pools and its size
select * from sys.sql_pools  --biazuresynapseworkspacenewpool
-----------------------------------------------------------------------------------------------------------------
--TO CHECK IF THE QUERY IS WAITING FOR SOME RESOURCE TO BE GRANTED
SELECT waits.session_id,waits.request_id, requests.command, requests.status, requests.start_time, waits.type, waits.object_type,waits.object_name, waits.state
FROM  sys.dm_pdw_waits waits
JOIN sys.dm_pdw_exec_requests requests  ON waits.request_id=requests.request_id
WHERE waits.request_id = 'QID350920225'
ORDER BY waits.object_name, waits.object_type, waits.state;
-----------------------------------------------------------------------------------------------------------------
--TO CHECK IF THE DEDICATED POOL IS TDS(TRANSPARENT DATA ENCRYPTED) OR NOT.
SELECT [name],[is_encrypted] FROM sys.databases;
-----------------------------------------------------------------------------------------------------------------
--TO VIEW THE SP DEFINITION:
SELECT  DEFINITION
FROM sys.sql_modules
WHERE object_id = (OBJECT_ID('SOH_SP_1_1_02_1and2'))
-----------------------------------------------------------------------------------------------------------------
--TO SEE HOW THE DATA IS LOADING, HOW MUCH DATA LOADED
SELECT r.[request_id],r.[status],r.resource_class,r.command
,sum(bytes_processed) AS bytes_processed,sum(rows_processed) AS rows_processed
FROM sys.dm_pdw_exec_requests r
JOIN sys.dm_pdw_dms_workers w
ON r.[request_id] = w.request_id
WHERE [label] = 'COPY: dbo.trip' and session_id <> session_id() and type = 'WRITER'
GROUP BY r.[request_id],r.[status],r.resource_class,r.command;
-----------------------------------------------------------------------------------------------------------------
--TO SEE QUERIES WHICH ARE EXCEEDING TIMEOUT OF WLM
select request_id,session_id,submit_time,start_time,DATEDIFF(mi,start_time,GETDATE()) as running_time,resource_class from sys.dm_pdw_exec_requests
where resource_class is not null and [status]='running' and DATEDIFF(mi,start_time,GETDATE()) >'75'
-----------------------------------------------------------------------------------------------------------------
--FIND DATA SKEW FOR A DISTRIBUTED TABLE
DBCC PDW_SHOWSPACEUSED('[customer_insights].[studio_metrics_daily_base_new]');
-----------------------------------------------------------------------------------------------------------------
--To check how much data is stored in the database
exec sp_spaceused
--space used by database:
--commerce-dev:23627528.69 MB  
--commerce	  :151495621.76 MB
--clikcstream :293980195.21 MB
-----------------------------------------------------------------------------------------------------------------
--To see latest snapshot(restore point)
select   top 1 * from     sys.pdw_loader_backup_runs
order by run_id desc;
-----------------------------------------------------------------------------------------------------------------
--Alter index of a table 
alter index all on clickstream.daily_aggregates rebuild
-----------------------------------------------------------------------------------------------------------------
--Update stats of a table
UPDATE STATISTICS clickstream.daily_aggregates	WITH SAMPLE 10 PERCENT;
-----------------------------------------------------------------------------------------------------------------
--Kill a session 
kill 'SID225039808'
-----------------------------------------------------------------------------------------------------------------
--To check segment range for a column of a segment
SELECT o.name, pnp.index_id, 
cls.row_count, pnp.data_compression_desc, 
pnp.pdw_node_id, pnp.distribution_id, cls.segment_id, 
cls.column_id, 
cls.min_data_id, cls.max_data_id, 
cls.max_data_id-cls.min_data_id as difference
FROM sys.pdw_nodes_partitions AS pnp
   JOIN sys.pdw_nodes_tables AS Ntables ON pnp.object_id = NTables.object_id AND pnp.pdw_node_id = NTables.pdw_node_id
   JOIN sys.pdw_table_mappings AS Tmap  ON NTables.name = TMap.physical_name AND substring(TMap.physical_name,40, 10) = pnp.distribution_id
   JOIN sys.objects AS o ON TMap.object_id = o.object_id
   JOIN sys.pdw_nodes_column_store_segments AS cls ON pnp.partition_id = cls.partition_id AND pnp.distribution_id  = cls.distribution_id
JOIN sys.columns as cols ON o.object_id = cols.object_id AND cls.column_id = cols.column_id
WHERE o.name = 'fact_core_item' and cols.name = '<Column Name>'  and TMap.physical_name  not like '%HdTable%'
ORDER BY o.name, pnp.distribution_id, cls.min_data_id;
-----------------------------------------------------------------------------------------------------------------
--To check lock on any TABLE
select * from sys.dm_pdw_lock_waits
where object_name = 'clickstream.daily_aggregates'
-----------------------------------------------------------------------------------------------------------------
--To check for ordered columns and order ordinal
SELECT object_name(c.object_id) table_name, c.name column_name, i.column_store_order_ordinal 
FROM sys.index_columns i 
JOIN sys.columns c ON i.object_id = c.object_id AND c.column_id = i.column_id
WHERE column_store_order_ordinal <>0;
-----------------------------------------------------------------------------------------------------------------
--To get the object id based on table and schema of a table
select t.object_id,t.name,s.name from sys.tables t join sys.schemas s
on t.schema_id=s.schema_id
where t.name like '%o_mk_catalogue_classification%'
-----------------------------------------------------------------------------------------------------------------
--To see table columns and its data type based on object id
select c.object_id,c.name,c.column_id,t.name as datatype from sys.columns c join sys.types t
on c.system_type_id=t.system_type_id
where object_id=object_id('dev.stu_tbs_stage4')
-----------------------------------------------------------------------------------------------------------------
--Query provides underlying distribution encryption status
SELECT keys.database_id AS DBIDinPhysicalDatabases,   
PD.pdw_node_id AS NodeID, PD.physical_name AS PhysDBName,   
keys.encryption_state  
FROM sys.dm_pdw_nodes_database_encryption_keys AS keys  
JOIN sys.pdw_nodes_pdw_physical_databases AS PD  
    ON keys.database_id = PD.database_id AND keys.pdw_node_id = PD.pdw_node_id  
ORDER BY keys.database_id, PD.pdw_node_ID;
-----------------------------------------------------------------------------------------------------------------
--To see encryption an decryption status
select * from sys.dm_pdw_nodes_database_encryption_keys 
order by percent_complete desc
-----------------------------------------------------------------------------------------------------------------
--Query provides the DW encryption status
SELECT D.database_id AS DBIDinMaster, D.name AS UserDatabaseName,   
PD.pdw_node_id AS NodeID, PD.physical_name AS PhysDBName,   
keys.encryption_state  
FROM sys.dm_pdw_nodes_database_encryption_keys AS keys  
JOIN sys.pdw_nodes_pdw_physical_databases AS PD  
    ON keys.database_id = PD.database_id AND keys.pdw_node_id = PD.pdw_node_id  
JOIN sys.databases AS D  
    ON D.database_id = PD.database_id  
ORDER BY D.database_id, PD.pdw_node_ID;
-----------------------------------------------------------------------------------------------------------------
--To see all system DMVs 
SELECT name,type,type_desc
FROM sys.system_objects so
WHERE so.name LIKE 'dm_%'
ORDER BY so.name;
-----------------------------------------------------------------------------------------------------------------
--SQLserver
SELECT *
FROM sys.dm_os_schedulers
WHERE scheduler_id < 255;
-----------------------------------------------------------------------------------------------------------------
--To search for the stored procedures
select * from sys.objects
where name like '%dim_date%' and type_desc='SQL_STORED_PROCEDURE'
-----------------------------------------------------------------------------------------------------------------
--To see stats on the TABLE
select * from sys.stats
where object_id in (select object_id from sys.objects where name='fact_core_item')
-----------------------------------------------------------------------------------------------------------------
--To see particular stats info
dbcc show_statistics('bidb.fact_core_item','_WA_Sys_00000053_7BF60B50')
-----------------------------------------------------------------------------------------------------------------
--To check when the stats were updated lst time on any TABLESELECT
    sm.[name] AS [schema_name],
    tb.[name] AS [table_name],
    co.[name] AS [stats_column_name],
    st.[name] AS [stats_name],
    STATS_DATE(st.[object_id],st.[stats_id]) AS [stats_last_updated_date]
FROM
    sys.objects ob
    JOIN sys.stats st
        ON  ob.[object_id] = st.[object_id]
    JOIN sys.stats_columns sc
        ON  st.[stats_id] = sc.[stats_id]
        AND st.[object_id] = sc.[object_id]
    JOIN sys.columns co
        ON  sc.[column_id] = co.[column_id]
        AND sc.[object_id] = co.[object_id]
    JOIN sys.types  ty
        ON  co.[user_type_id] = ty.[user_type_id]
    JOIN sys.tables tb
        ON  co.[object_id] = tb.[object_id]
    JOIN sys.schemas sm
        ON  tb.[schema_id] = sm.[schema_id]
WHERE
    st.[user_created] = 1
	and ob.object_id=object_id('[bidb].[fact_style_funnel_clickstream]');
-----------------------------------------------------------------------------------------------------------------
--To check queries fired from anyone of the login
select command,count(*) from sys.dm_pdw_exec_requests
where session_id in (select session_id from sys.dm_pdw_exec_sessions
where status ='active' and login_name='udp_myntrauser')
group by command
-----------------------------------------------------------------------------------------------------------------
--To check execution time of completed commands 
select  request_id,datediff(mi,start_time,end_time) as exec_time_min,command from sys.dm_pdw_exec_requests
where datediff(s,start_time,end_time)>60
order by 2 desc
-----------------------------------------------------------------------------------------------------------------
--To get average execution time of a cluster
select sum(datediff(mi,start_time,end_time))/sum(case when datediff(s,start_time,end_time)>60 then 1 else 0 end) as avg_exec_time
from sys.dm_pdw_exec_requests
where status='completed'
-----------------------------------------------------------------------------------------------------------------
--Node memory usage 
--The following query will show how much memory is currently in use by each SQLDW node compared to the max for current DWU
SELECT 
	pc1.cntr_value as Curr_Mem_KB, pc1.cntr_value/1024.0 as Curr_Mem_MB, 	(pc1.cntr_value/1048576.0) as Curr_Mem_GB, 
	pc2.cntr_value as Max_Mem_KB, 
	pc2.cntr_value/1024.0 as Max_Mem_MB, 
	(pc2.cntr_value/1048576.0) as Max_Mem_GB, 
	pc1.cntr_value * 100.0/pc2.cntr_value AS Memory_Utilization_Percentage, 
	pc1.pdw_node_id 
FROM
	sys.dm_pdw_nodes_os_performance_counters AS pc1 
	JOIN sys.dm_pdw_nodes_os_performance_counters AS pc2 ON pc1.object_name = pc2.object_name 
	AND pc1.pdw_node_id = pc2.pdw_node_id 
WHERE pc1.counter_name = 'Total Server Memory (KB)' AND pc2.counter_name = 'Target Server Memory (KB)'
order by 7 desc
-----------------------------------------------------------------------------------------------------------------
SELECT 
	pc1.pdw_node_id ,
	cast((pc1.cntr_value * 100.0/pc2.cntr_value) as float) AS Memory_Utilization_Percentage
FROM
	sys.dm_pdw_nodes_os_performance_counters AS pc1 
	JOIN sys.dm_pdw_nodes_os_performance_counters AS pc2 ON pc1.object_name = pc2.object_name 
	AND pc1.pdw_node_id = pc2.pdw_node_id 
WHERE pc1.counter_name = 'Total Server Memory (KB)' AND pc2.counter_name = 'Target Server Memory (KB)'
order by 2 desc

--Granted concurrency slots 
--The following query will show how many conccurency slots are currently granted
SELECT  SUM([concurrency_slots_used]) as total_granted_slots 
FROM    sys.[dm_pdw_resource_waits] 
WHERE   [state]           = 'Granted' 
AND     [resource_class] is not null
AND     [session_id]     <> session_id()
-----------------------------------------------------------------------------------------------------------------
--DMS heavy hitters
--The following query returns queries that are currently processing rows in DMS
WITH step_data AS 
(
	SELECT 
		SUM(rows_processed) AS Step_Rows_Processed
		, SUM(Bytes_processed) AS Step_Bytes_Processed
		, request_id
		, step_index
		, status 
		, type
	FROM sys.dm_pdw_dms_workers 
	WHERE status != 'StepComplete'
	Group by 
		  request_id
		, step_index
		, status
		, type
) 
SELECT 
	 step_data.request_id
	, per.session_id
	, step_data.Step_rows_processed
	, step_data.step_bytes_processed
	, (step_data.Step_Bytes_Processed/1024/1024/1024) AS Step_GB_Processed
	, step_data.step_index
	, step_data.type
	, step_data.status as Step_status
	, per.status AS QID_Status
	, per.total_elapsed_time/1000/60 as 'QID_Elapsed_Time (min)'
	, per.command AS 'QID_Command'
	, per.resource_class
	, per.importance
FROM step_data
LEFT JOIN sys.dm_pdw_exec_requests per
ON step_data.request_id = per.request_id
WHERE per.status = 'Running'
ORDER BY step_data.Step_rows_processed DESC
-----------------------------------------------------------------------------------------------------------------
--Identify resource waits
SELECT  w.[wait_id]
,       w.[session_id]
,       w.[type]                                           AS Wait_type
,       w.[object_type]
,       w.[object_name]
,       w.[request_id]
,       w.[request_time]
,       w.[acquire_time]
,       w.[state]
,       w.[priority]
,       SESSION_ID()                                       AS Current_session
,       s.[status]                                         AS Session_status
,       s.[login_name]
,       s.[query_count]
,       s.[client_id]
,       s.[sql_spid]
,       r.[command]                                        AS Request_command
,       r.[label]
,       r.[status]                                         AS Request_status
,       r.[submit_time]
,       r.[start_time]
,       r.[end_compile_time]
,       r.[end_time]
,       DATEDIFF(ms,r.[submit_time],r.[start_time])        AS Request_queue_time_ms
,       DATEDIFF(ms,r.[start_time],r.[end_compile_time])   AS Request_compile_time_ms
,       DATEDIFF(ms,r.[end_compile_time],r.[end_time])     AS Request_execution_time_ms
,       r.[total_elapsed_time]
FROM    sys.dm_pdw_waits w
JOIN    sys.dm_pdw_exec_sessions s  ON w.[session_id] = s.[session_id]
JOIN    sys.dm_pdw_exec_requests r  ON w.[request_id] = r.[request_id]
WHERE    w.[session_id] <> SESSION_ID() 
-----------------------------------------------------------------------------------------------------------------
--Queued query TIME
SELECT  r.[request_id]                           AS Request_ID
,       r.[status]                               AS Request_Status
,       r.[submit_time]                          AS Request_SubmitTime
,       r.[start_time]                           AS Request_StartTime
,       DATEDIFF(ms,[submit_time],[start_time])  AS Request_InitiateDuration_ms
,       r.resource_class                         AS Request_resource_class
FROM    sys.dm_pdw_exec_requests r
ORDER BY Request_InitiateDuration_ms desc
-----------------------------------------------------------------------------------------------------------------
select * from sys.dm_pdw_errors
where year(create_time)=2022 and month(create_time)=05 and day(create_time)=24
order by create_time desc
-----------------------------------------------------------------------------------------------------------------
exec sp_concurrency
exec sp_requests
exec sp_waits
exec sp_status
exec sp_datamovement
exec sp_requests_detail ''
exec sp_waits_detail ''

exec sp_droprolemember 'largerc', 'load_user';
exec sp_addrolemember 'mediumrc', 'load_user';


SELECT CASE WHEN cluster_utilization > 100 OR total_queries > 30 OR running_queries > 100 THEN 1 ELSE 0 END AS send_mail
FROM(
SELECT SUM(COALESCE(resource_allocation_percentage,0)) as cluster_utilization, count(*) as total_queries,
SUM(CASE WHEN start_time IS NOT NULL THEN 1 ELSE 0 END) as running_queries, SUM(CASE WHEN start_time IS NULL THEN 1 ELSE 0 END) as queued_queries,
AVG(DATEDIFF ( MINUTE , start_time , COALESCE(end_time,CURRENT_TIMESTAMP) )) as evg_exec_time
FROM sys.dm_pdw_exec_requests WHERE status not in ('Completed','Failed','Cancelled')  AND resource_class IS NOT NULL
) a;
------------------------------------------------------------------------------------------------------------------
--count number of tables having different distributions
SELECT 
distribution_policy_desc,count(*) count()
FROM sys.pdw_table_distribution_properties
group by distribution_policy_desc


--Run this query in master database to get all the users which are associated with logins
select l.name as [login name],u.name as [user name] from sysusers u inner join sys.sql_logins l on u.sid=l.sid

--Redundant tables
select * from information_schema.tables
where table_name like '%dev%' or table_name like '%temp%' or table_name like '%bak%' and table_type<>'VIEW'

--Not an external user
select name as username,create_date,modify_date,type_desc as type,authentication_type_desc as authentication_type
from sys.database_principals
where type not in ('A', 'G', 'R', 'X') and sid is not null and type_desc<>'EXTERNAL_USER'
order by username;

-------------------------------------------------------------------------------------------
--Query to find tables which have zero records but still have space on data file
select two_part_name,sum(row_count) row_count,sum(reserved_space_KB) reserved_space_kb,sum(unused_space_kb) unused_spcae_kb,
sum(data_space_kb) data_space_kb
from synapseanalyzer.tablesizes
group by two_part_name
having sum(row_count)=0
order by 2 asc,3 desc
-------------------------------------------------------------------------------------------
--Query to find tables that are not used recently
with UnUsedTables (TableName , TotalRowCount, CreatedDate , LastModifiedDate ) 
AS ( 
  SELECT DBTable.name AS TableName
     ,PS.row_count AS TotalRowCount
     ,DBTable.create_date AS CreatedDate
     ,DBTable.modify_date AS LastModifiedDate
  FROM sys.all_objects  DBTable 
     JOIN sys.dm_pdw_nodes_db_partition_stats PS ON OBJECT_NAME(PS.object_id)=DBTable.name
  WHERE DBTable.type ='U' 
     AND NOT EXISTS (SELECT OBJECT_ID  
                     FROM sys.dm_pdw_nodes_db_index_usage_stats
                     WHERE OBJECT_ID = DBTable.object_id )
)
-- Select data from the CTE
SELECT TableName , TotalRowCount, CreatedDate , LastModifiedDate 
FROM UnUsedTables
ORDER BY TotalRowCount ASC
-------------------------------------------------------------------------------------------
--To count tables in each distribution policy
select (case when distribution_policy=2 then 'Hash' when distribution_policy=3 then 'replicate' when distribution_policy=4 then 'Round Robin' else 'None/Undefined' end)
as Distribution,count(*) total_tables from sys.pdw_table_distribution_properties
group by distribution_policy
order by 2 desc
-------------------------------------------------------------------------------------------
-- Memory consumption
SELECT
  pc1.cntr_value as Curr_Mem_KB,
  pc1.cntr_value/1024.0 as Curr_Mem_MB,
  (pc1.cntr_value/1048576.0) as Curr_Mem_GB,
  pc2.cntr_value as Max_Mem_KB,
  pc2.cntr_value/1024.0 as Max_Mem_MB,
  (pc2.cntr_value/1048576.0) as Max_Mem_GB,
  pc1.cntr_value * 100.0/pc2.cntr_value AS Memory_Utilization_Percentage,
  pc1.pdw_node_id
  FROM
-- pc1: current memory
sys.dm_pdw_nodes_os_performance_counters AS pc1
-- pc2: total memory allowed for this SQL instance
JOIN sys.dm_pdw_nodes_os_performance_counters AS pc2
ON pc1.object_name = pc2.object_name AND pc1.pdw_node_id = pc2.pdw_node_id
WHERE
pc1.counter_name = 'Total Server Memory (KB)'
AND pc2.counter_name = 'Target Server Memory (KB)'
order by 7 desc
-------------------------------------------------------------------------------------------
-- Transaction log size
SELECT
  instance_name as distribution_db,
  sum(cntr_value*1.0/1048576) as log_file_size_used_GB,
  count(pdw_node_id)
FROM sys.dm_pdw_nodes_os_performance_counters
WHERE
instance_name like 'Distribution_%'
AND counter_name = 'Log File(s) Used Size (KB)'
group by instance_name
order by 2 desc
-------------------------------------------------------------------------------------------
--To delete duplicates records
WITH CTE(id,
    DuplicateCount)
AS (SELECT id,
           ROW_NUMBER() OVER(PARTITION BY id
           ORDER BY id) AS DuplicateCount
    FROM bidb.o_item)
DELETE FROM CTE
--select count(*) from cte 
WHERE DuplicateCount > 1;
-------------------------------------------------------------------------------------------
--alter workload group
Alter WORKLOAD GROUP wg_hflr_myntrapoc
 WITH
 (   MIN_PERCENTAGE_RESOURCE = 0
   , CAP_PERCENTAGE_RESOURCE = 18
   , REQUEST_MIN_RESOURCE_GRANT_PERCENT =3
 ,  REQUEST_MAX_RESOURCE_GRANT_PERCENT = 3
  ,  IMPORTANCE =  ABOVE_NORMAL
   ,  QUERY_EXECUTION_TIMEOUT_SEC = 3600  )
-------------------------------------------------------------------------------------------
