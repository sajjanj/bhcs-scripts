#!/usr/bin/ksh
# Script Name : cycle_end_success.sh
# Author      : Sajjan Janardhanan
# Created On  : 06/01/2012
# Description : Make entries in the new ETL control table T_ETL_PROCESS_AUDIT under the ETLSVC schema in the EDW database
#               Parameter sequence = 1.ProcessType (INFA,ORCL,UNIX,OTH)
#                                    2.ProcessName (Workflow Name, SP Name, Script Name)
#                                    3.SubjectArea (Folder, Schema, Project name etc.)
#
# Revision History
# ~~~~~~~~~~~~~~~~
# Rev-Dt        Rev-By                Rev-Desc
# 06/01/2012    Sajjan Janardhanan    created new
# 08/08/2012    Sajjan Janardhanan    changes related to the new structure of the ETL control table

. /dw001/app/edwetl/scripts/common/etl_func_env.sh

# Local Variables
action_name="SUCCESS"
bin_dir=${ETL_SCRIPTS}/common
log_file_dir=${bin_dir}/log
notify_list=sajjan.janardhanan@BaylorHealth.edu
now=`date +%Y-%m-%d_%T`
process_type_lst="INFA,ORCL,UNIX,OTH"
script_nm=`basename $0` 
std_abort_msg="The process has encountered a fatal error and will now abort. Pls contact EDW support.\n"
today=`date +%m-%d-%Y` # <== exact format(mm-dd-yyyy)
log_file_nm="${log_file_dir}/${script_nm}_${1}-${2}_${now}.log"

{ # <== logging mechanism begin ==

	# Environment identification
	server_name=`uname -a|awk '{ print $2 }'`
	case $server_name in
	  etldev ) env="[ETL DEV ]";;
	  etltst ) env="[ETL TEST]";;
	  etlprd ) env="[ETL PROD]";;
	  *      ) env="[Unknown ]";;
	esac

	echo "Environment              = $env"
	echo "Run Timestamp            = $now"
	echo "Run by User              = [`whoami`]"
	echo "Server information       = `uname -a`"
	echo "Script Location & Name   = $bin_dir/$script_nm"
	echo "Script Log File Name     = $log_file_nm "
	
	# Checking parameters
	total_parms=$#
	if [ $total_parms -eq 3 ]; then
		process_type=$1
		process_name=$2
		subject_area=$3
		echo "[`date +%Y-%m-%d_%T`] Parameter check =>"
		echo "                      Process Type = [${process_type}]"
		echo "                      Process Name = [${process_name}]"
		echo "                      Subject Area = [${subject_area}]"
		process_type_chk=`echo $process_type_lst|grep -w $process_type`
		if [ $process_type_chk != $process_type_lst ]; then
			echo "[`date +%Y-%m-%d_%T`] ERROR: Process Type is not supported."
			echo "[`date +%Y-%m-%d_%T`] $std_abort_msg"				
			exit 1
		fi
	else
		echo "[`date +%Y-%m-%d_%T`] ERROR: Insufficient parameters."
		echo "[`date +%Y-%m-%d_%T`] $std_abort_msg"		
		exit 2
	fi

	${bin_dir}/cycle_main.sh ${process_type} ${process_name} ${subject_area} ${action_name}
	
	return_value=$?
	if [ ${return_value} -ne 0 ]; then
		echo "\n[`date +%Y-%m-%d_%T`] ERROR: Process completed in failure. Ref# ${return_value}"
		echo "[`date +%Y-%m-%d_%T`] $std_abort_msg"
		return ${return_value}
	fi
	
	echo "\n[`date +%Y-%m-%d_%T`] The script execution has completed successfully."

} > $log_file_nm 2>&1 # <== logging mechanism end ==

return 0