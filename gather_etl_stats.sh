#!/usr/bin/ksh
# Script Name : gather_etl_stats.sh
# Author      : Sajjan Janardhanan
# Created On  : 06/12/2012
# Description : Fetches the following load statistics for a particular session 
#               within a workflow from the MX views within the INFA repository
#               - Source Rows Applied  (aka) SUCCESSFUL_SOURCE_ROWS
#               - Target Rows Affected (aka) SUCCESSFUL_ROWS
#               - Target Rows Failed   (aka) FAILED_ROWS
#               - First Error Message  (aka) FIRST_ERROR_MSG
#               - Start Time           (aka) ACTUAL_START
#               - End Time             (aka) SESSION_TIMESTAMP
#
# Revision History
# ~~~~~~~~~~~~~~~~
# Rev-Dt        Rev-By                Rev-Desc
# 06/12/2012    Sajjan Janardhanan    created new

total_parms=$#
if [ $total_parms -eq 3 ]; then
	subject_area=$1	
	workflow_name=$2
	session_name=$3
elif [ $total_parms -gt 1 ]; then
	param_value=$1	
	debug_mode=$2
fi

. /dw001/app/edwetl/scripts/common/etl_func_env.sh
function_get_logon etlsvc

# Environment identification
server_name=`uname -a|awk '{ print $2 }'`
case $server_name in
  etldev ) env="[ETL DEV ]";;
  etltst ) env="[ETL TEST]";;
  etlprd ) env="[ETL PROD]";;
  *      ) env="[Unknown ]";;
esac

# Local Variables
scheduled_run=y        # <== change to "y" in PROD
today=`date +%m-%d-%Y` # <== exact format(mm-dd-yyyy)
script_nm=`basename $0` #`echo $0 | sed -e "s/^.\/*//"`
bin_dir=${ETL_SCRIPTS}/common
now=`date +%Y-%m-%d_%T`
log_file_dir=${ETL_SCRIPTS}/common/log
log_file_nm="${log_file_dir}/${script_nm}_${param_value}_${now}.log"
std_abort_msg="The process has encountered a fatal error and will now abort. Pls contact EDW support.\n"
notify_list=sajjan.janardhanan@BaylorHealth.edu
err_occured="n"

{ # <== logging mechanism begin ==

    echo "Server information       = `uname -a`"
	echo "Script Location & Name   = $bin_dir/$script_nm"
	echo "Environment              = $env"
	echo "Run Timestamp            = $now"
	echo "Run by User              = [`whoami`]"
	echo "Script Log File Name     = $log_file_nm "

	echo "Subject Area             = $subject_area "
	echo "Workflow Name            = $workflow_name "
	echo "Session Name             = $session_name "

# call procedure to load data from the INFAPROD into EDWPROD control table
	
record=`sqlplus -s << SQLBLOCK 
etlsvc/${ORA_PASSWD}@${ORA_DB}
set feed off
set head off
set pages 0
set pagesize 0
set serverout on
set tab off
set ver off
WHENEVER SQLERROR EXIT 9
select SUCCESSFUL_SOURCE_ROWS || '~' || FAILED_ROWS || '~' || FIRST_ERROR_MSG || '~' || ACTUAL_START || '~' || SESSION_TIMESTAMP
from REP_SESS_LOG OQ where SUBJECT_AREA='$subject_area' and WORKFLOW_NAME='$workflow_name' and SESSION_NAME='$session_name'
where WORKFLOW_RUN_ID = (select max(WORKFLOW_RUN_ID) from REP_SESS_LOG IQ where IQ.WORKFLOW_NAME = OQ.WORKFLOW_NAME)
EXIT;
SQLBLOCK`

successful_source_rows=`echo ${record}|cut -d "," -f 1`
target_successful_rows=`echo ${record}|cut -d "," -f 2`
target_failed_rows=`echo ${record}|cut -d "," -f 3`
first_error_msg=`echo ${record}|cut -d "," -f 4`
actual_start=`echo ${record}|cut -d "," -f 5`
session_timestamp=`echo ${record}|cut -d "," -f 6`

sqlplus -s stg/${ORA_PASSWD}@${ORA_DB}<<!
set serveroutput on
set heading off feedback off 
spool ${log_file_dir}/gather_etl_stats_${now}.spl

insert into stg.edw_ctl_tbl values  ...

/
commit;

set linesize 1300 trimspool on heading off echo off term off pagesize 0 feedback
 off timing off pause off verify off


/
spool off
!

} > $log_file_nm 2>&1 # <== logging mechanism end ==

return 0






