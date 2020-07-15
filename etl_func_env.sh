#!/bin/sh
##################################################################################
# Script Name: etl_func_env.sh
# Author: Vincent Kiango 
# Desc: Environment sh functions for the ETL process
#=================================================================================
# Arguments:
#      etl_func_env.sh <NOTHING>
#     Example:  ./etl_func_env.sh"
#================================================================================
# Modification History:
# Date   Changed by     Description
# 2/16   VK             Added function_purge
# 1/29   SJ             Added function_archive_file & added email addresses
#i################################################################################

. ${HOME}/.profile

#==================================================================================
# Set ETL environment variables 
#==================================================================================
ETL_HOME=/dw001/app/edwetl
export ETL_HOME
ETL_SCRIPTS=${ETL_HOME}/scripts
export ETL_SCRIPTS 
LOGONS_DIR=${ETL_SCRIPTS}/logons
export LOGONS_DIR 
LOGON_FILE=${LOGONS_DIR}/.etl.logons
export LOGON_FILE 
ETL_INBOUND=/dw001/app/edwetl/inbound
export ETL_INBOUND 
ETL_OUTBOUND=/dw001/app/edwetl/outbound
export ETL_OUTBOUND 
ETL_ARC_INBOUND=/dw001/app/edwetl/archive/inbound
export ETL_ARC_INBOUND 
ETL_ARC_OUTBOUND=/dw001/app/edwetl/archive/outbound
export ETL_ARC_OUTBOUND 

#==================================================================================
# set maillist for notifications 
#==================================================================================
HEWITT_DATA_QLTY="edwetlsupport@baylorhealth.edu,HewittDataQuality@baylorhealth.edu,JaredA.Hooste@baylorhealth.edu"
export HEWITT_DATA_QLTY 
EDW_MAILLIST="edwetlsupport@baylorhealth.edu"
export EDW_MAILLIST
CEO_FIN_QLT_MAILLIST="edwetlsupport@baylorhealth.edu,ceofinancedataquality@baylorhealth.edu"
export CEO_FIN_QLT_MAILLIST 
NRC_QLT_MAILLIST="edwetlsupport@baylorhealth.edu,CustomerPreference-NRCDataQuality@baylorhealth.edu,tickerbaylorteam@nationalresearch.com"
export NRC_QLT_MAILLIST 
TRENDSTAR_MAILLIST="edwetlsupport@BaylorHealth.edu,CarlM@BaylorHealth.edu,JeffN@BaylorHealth.edu,fredm@BaylorHealth.edu,ScottAd@BaylorHealth.edu"
export TRENDSTAR_MAILLIST 
VMR_MAILLIST="vmrdataquality@bhcs.com,edwetlsupport@baylorhealth.edu"
export VMR_MAILLIST 
PGANEY_MAILLIST="edwetlsupport@baylorhealth.edu,chizuko.hastings@bhcs.com,Kathryn.schmidt@bhcs.com,Jenh@bhcs.com,Stevan.james@bhcs.com,Samantha.Baldwin@baylorhealth.edu,Arthur.Williams@baylorhealth.edu,Bonnie.McCamey@baylorhealth.edu"
export PGANEY_MAILLIST
 
#=================================================================================
# Function    : function_get_logon
# Parameters  : sql user/schema name
# Return      : none
# Description : Get password for sql user schema name and database name.
#==================================================================================
function_get_logon()
 {
  ORA_USER=$1
  export ORA_USER

  LOGON_ID="`uname -n`|ora|${ORA_USER}"
  export ORA_PASSWD=`grep -iw ${LOGON_ID} ${LOGON_FILE} | cut -f4 -d'|'`
  export ORA_DB=`grep -iw ${LOGON_ID} ${LOGON_FILE} | cut -f5 -d'|'`

  return
 }

#==================================================================================
# Function      :function_purge
# Parameters  : Directory and archive limit. e.g function_purge ${ETL_ARC_INBOUND}/patcom 27
# Return         : none
# Description   :Remove sub directories and files from archive directory based on
#                archive limit.
#==================================================================================
function_purge()
{
        DIR=$1
        ARCHLIMIT=$2

       for OLD_DIR in `find ${DIR}/* -type d -prune -mtime +${ARCHLIMIT} -print`
       do
        if [ -d "$OLD_DIR" ]; then
         rm -rf ${OLD_DIR}
        fi
      done
      find ${DIR}/* -mtime +${ARCHLIMIT} -exec rm {} \;
}


#==================================================================================
# Function      :function_notify 
# Parameters  : Content, Subject and Mail address
# Return         : none
# Description   :Function used to trigger email notification
#==================================================================================
function_notify()
{
        CONTENT=$1
        SUBJECT=$2
        MAILTO=$3
        	echo $CONTENT | mailx -s "$SUBJECT" $MAILTO
        
}

#==================================================================================
# Function    : function_log
# Parameters  : log type [INFO], log text
# Return      : none
# Description : Displays common log output and exits on [ERROR] with email
#               notification.
#               
#==================================================================================
function_log()
 {
  if [[ -z $1 ]] || [[ -z $2 ]]
  then
    echo "[WHAT?]:[`date`]:$1 $2"
  else
    echo "[$1]:[`date`]:$2" | tee -a ${LOG_FILE_NM}
    if [ "$1" = "ERROR" ]
    then
      P_CONTENT="Error: $2 at `date`.For more information please refer log file: ${LOG_FILE_NM}"
      P_SUBJECT="Error at `uname -n` server on the program name:${PRGM_NM}"
      function_notify "$P_CONTENT" "$P_SUBJECT" "$MAILLIST"
     exit 1 
    fi
  fi
  return
 }
#==================================================================================
# Function    : function_orasql
# Parameters  : sql user/schema name, sql to submit, sql error log text
# Return      : SQLVAR
# Description : Executes oracle SQL with a single row and column return value
#==================================================================================
function_orasql()
 {

  if [[ -z $1 ]] || [[ -z $2 ]] || [[ -z $3 ]]
  then
    function_log "INFO" "function_orasql(3) $1:$2:$3"
    SQLVAR=""
  else
    ORA_USER=$1
    SQLSUBMIT=$2
    SQLERROR=$3
    if [ ${SHOWSQL} = "Y" ]
    then
      function_log "SQL=" "${SQLSUBMIT}"
    fi

export ORA_USER=$1

LOGON_ID="`uname -n`|ora|${ORA_USER}"
export ORA_PASSWD=`grep -iw ${LOGON_ID} ${LOGON_FILE} | cut -f4 -d'|'`
export ORA_DB=`grep -iw ${LOGON_ID} ${LOGON_FILE} | cut -f5 -d'|'`

    SQLVAR=`$ORACLE_HOME/bin/sqlplus -s $ORA_USER/$ORA_PASSWD@$ORA_DB << !!
      SET PAGESIZE 0
      SET FEEDBACK OFF
      SET ECHO OFF
      SET VERIFY OFF
      SET DEFINE OFF
      WHENEVER SQLERROR EXIT SQL.SQLCODE;
      start ${SQLSUBMIT};
      EXIT;
    !!`
    status=$?
    if [ "${SQLERROR}" != "DO_NOT_TEST_FOR_ERROR" ]
    then
      if [ ${status} != 0 ]
      then
       function_log "ERROR" "${SQLERROR}"
      fi
    fi
  fi
  return
 }

#==================================================================================
# Function    : function_orasql_commit
# Parameters  : sql user/schema name, sql to submit, sql error log text
# Return      : none
# Description : Executes oracle SQL transaction(s) to be committed.
#==================================================================================
function_orasql_commit()
 {
  if [[ -z $1 ]] || [[ -z $2 ]] || [[ -z $3 ]]
  then
    function_log "function_orasql_commit(3) $1:$2:$3"
  else
    ORA_USER=$1
    SQLSUBMIT=$2
    SQLERROR=$3
    if [ ${SHOWSQL} = "Y" ]
    then
      function_log "SQL=" "${SQLSUBMIT}"
    fi

 export ORA_USER=$1

 LOGON_ID="`uname -n`|ora|${ORA_USER}"
 export ORA_PASSWD=`grep -iw ${LOGON_ID} ${LOGON_FILE} | cut -f4 -d'|'`
 export ORA_DB=`grep -iw ${LOGON_ID} ${LOGON_FILE} | cut -f5 -d'|'`
 
    $ORACLE_HOME/bin/sqlplus -s $ORA_USER/$ORA_PASSWD@$ORA_DB << !!
      SET PAGESIZE 0
      SET FEEDBACK OFF
      SET ECHO OFF
      SET VERIFY OFF
      SET DEFINE OFF
      WHENEVER SQLERROR EXIT SQL.SQLCODE;
      start ${SQLSUBMIT};
      COMMIT;
      EXIT;
!!
    status=$?
    if [ "${SQLERROR}" != "DO_NOT_TEST_FOR_ERROR" ]
    then
      if [ ${status} != 0 ]
      then
       function_log "ERROR" "${SQLERROR}"
      fi
    fi
  fi
  return
 }

#==================================================================================
# Function    : function_orasql_spool
# Parameters  : sql user/schema name, sql to submit, sql error log text, file name
# Return      : none
# Description : Executes oracle SQL transaction(s) to be spooled to a file.
#==================================================================================
function_orasql_spool()
 {
  if [[ -z $1 ]] || [[ -z $2 ]] || [[ -z $3 ]] || [[ -z $4 ]]
  then
    function_log "function_orasql_spool(4) $1:$2:$3:$4"
  else
    ORA_USER=$1
    SQLSUBMIT=$2
    SQLERROR=$3
    SQLFILE=$4
    if [ ${SHOWSQL} = "Y" ]
    then
      function_log "SQL=" "${SQLSUBMIT}"
    fi

   export ORA_USER=$1

 LOGON_ID="`uname -n`|ora|${ORA_USER}"
 export ORA_PASSWD=`grep -iw ${LOGON_ID} ${LOGON_FILE} | cut -f4 -d'|'`
 export ORA_DB=`grep -iw ${LOGON_ID} ${LOGON_FILE} | cut -f5 -d'|'`

 $ORACLE_HOME/bin/sqlplus -s $ORA_USER/$ORA_PASSWD@$ORA_DB << !! > /dev/null
      SET ECHO OFF                                  SET NEWPAGE NONE   
      SET NEWPAGE 0                                 SET VERIFY OFF     
      SET SPACE 0                                   SET LINESIZE 220   
      SET PAGESIZE 0                                SET PAGESIZE 0     
      SET FEEDBACK OFF                              SET ECHO ON        
      SET HEADING OFF                               SET FEEDBACK ON    
      SET TRIMSPOOL ON                              SET HEADING OFF    
      SET TAB OFF                                   SET TRIMSPOOL ON   
      SET LINESIZE 32767                            SET TERMOUT ON     
      SET TERMOUT OFF                               SET MARKUP HTML OFF
      SET VERIFY OFF                                WHENEVER SQLERROR EXIT SQL.SQLCODE;
      SET DEFINE OFF
      WHENEVER OSERROR EXIT FAILURE
      WHENEVER SQLERROR EXIT SQL.SQLCODE
      SPOOL ${SQLFILE}
      start ${SQLSUBMIT};
      SPOOL OFF
      EXIT;
!!
    status=$?
    if [ "${SQLERROR}" != "DO_NOT_TEST_FOR_ERROR" ]
    then
      if [ ${status} != 0 ]
      then
        function_log "ERROR" "${SQLERROR}"
      fi
    fi
  fi
  return
 }

#==================================================================================
# Function    : function_get_db_pswd
# Parameters  : sql user/db name
# Return      : none
# Description : Get password for sql user schema name and database name.
# Author      : Sajjan Janardhanan
# Date        : 2012-07-20
#==================================================================================
function_get_db_pswd()
 {
  if [ $# -ne 2 ]; then
    echo "ERROR: function_get_db_pswd() - Insufficient parameters."
    return -1
  else
    ORA_USER=$1
	export ORA_USER
    ORA_DB=$2
	export ORA_DB
	ORA_PSWD=`grep -iw ${ORA_USER} ${LOGON_FILE} | grep -iw ${ORA_DB}| cut -f4 -d'|'`
	export ORA_PSWD
    return 0
  fi
 }

#==================================================================================
# Function    : function_identify_env
# Parameters  : None
# Return      : None, but sets global variable ENV
# Description : Sets the name of the environment to the global variable
# Author      : Sajjan Janardhanan
# Date        : 2012-07-20
#==================================================================================
function_identify_env()
{
	server_name=`uname -a|awk '{ print $2 }'`
	if [ $? != 0 ]; then
		echo "ERROR #1 in [function_identify_server]"; return 1
	fi
	case $server_name in
		etldev ) ENV="[ETL DEV]";EDWDB="EDWDEV_BDWDEV";;
		etltst ) ENV="[ETL TEST]";EDWDB="EDWTST_BDWTEST";;
		etlprd ) ENV="[ETL PROD]";EDWDB="EDWPRD_BDWPROD";;
		*      ) ENV="[Unknown]";EDWDB="[Unknown]";;
	esac
	if [ $? -ne 0 ]; then
		echo "ERROR #2 in [function_identify_env]"; return 1
	fi	
	
	export ENV
	export EDWDB
	return 0
}

#==================================================================================
# Function    : function_archive_file
# Parameters  : 3 (1 = folder path, 2 = file name, 3 = debug mode)
# Return      : None
# Description : Archives files within a subfolder created with the current date
# Author      : Sajjan Janardhanan
# Date        : 2012-10-25
#==================================================================================
function_archive_file()
{
	dir_archive=$1
	file_src_abspath=$2
	if [ $# -eq 3 ]; then
		yn_debug_mode=$3
	else
		yn_debug_mode="n"
	fi
	
	dir_subfolder=`date +%Y-%m-%d`
	if [ ! -d $dir_archive/$dir_subfolder ]; then
		mkdir ${dir_archive}/${dir_subfolder}
		if [ $? -ne 0 ]; then
			echo "ERROR #1 in [function_archive_file]"; return 1
		fi
		chmod 754 $dir_archive/$dir_subfolder
	fi

	cp -f ${file_src_abspath} ${dir_archive}/${dir_subfolder}
	if [ $? -ne 0 ]; then
		echo "ERROR #2 in [function_archive_file]"; return 1
	fi

	if [ ${yn_debug_mode} == "n" ]; then
		rm -f $file_src_abspath 
		if [ $? -ne 0 ]; then
			echo "ERROR #3 in [function_archive_file]"; return 1
		fi
	fi
	
	return 0
}