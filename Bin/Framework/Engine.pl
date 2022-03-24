#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  Name:		Engine.pl
#  IN_parameters:	-eng engine_id [-x debug_level] [-debug]
#  OUT_paramaters:	exit_cd
#  Called from:		Windows scheduler every five minutes
#  Calling:		SP_ENG_GET_JOB_LIST
#  			SP_ENG_******
#-------------------------------------------------------------------------------
#  Project:		PDC
#  Author:		Teradata - Petr Stefanek
#  Date:		2011-09-08
#-------------------------------------------------------------------------------
#  Version:		2.0
#-------------------------------------------------------------------------------
#  Description:		Script runs the jobs
#-------------------------------------------------------------------------------
#  Version:		2.0
#  Modified:		PSt
#  Date:		2015-02-24
#  Modification:	Double Engine support
#-------------------------------------------------------------------------------
#  Version:		2.1
#  Modified:		MBU
#  Date:		2016-06-15
#  Modification:	better handling with PID
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

# Script reads the job list from database and execute jobs from the list

use Getopt::Long;
use DBI;
# We need DBD::Oracle data types for REF Cursor variable
use DBD::Oracle qw(:ora_types);
use XML::Simple;
use File::Spec;

if (uc($^O) eq "MSWIN32") {
	require Win32::Process;
	require Win32;
}
elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
	require threads;
}
else {
	print "ERROR> Unsupported operating system, exiting...\n";
	exit 9;
}

GetOptions(
	"engine=i"=> \$ENGINE_ID,
	"x=i"=> \$DEBUG,
	"debug"=> \$DEBUGFLAG
	);
if (not defined $ENGINE_ID) { $ENGINE_ID = 0; }
if (not defined $DEBUG) { $DEBUG = 8; }
if ($DEBUG > 8) {
	$DEBUG = 8; 			# don't comment this line from security reason
}
if (not defined $DEBUGFLAG) {
	$DEBUGFLAG = 0;
	$debugflag = "";
}
else {
	$debugflag = "-debug";
}
($DEBUG > 4) and print "CHECK> ENGINE_ID: $ENGINE_ID\n";
($DEBUG > 4) and print "CHECK>     DEBUG: $DEBUG\n";

print "Unprocessed by Getopt::Long\n" if $ARGV[0];
foreach (@ARGV) {
	print "$_\n";
}

################################################################################
# Konfigurovatelne parametry
#

$SLEEPING_TIME = 5;				# duration of waiting if no executable job is found (default = 5 seconds)
$MAX_CYCLE = 999;				# number of execution of procedura SP_ENG_GET_JOB_LIST within one session
$DELAY_AFTER_ERROR = 5;				# duration of retention period after error (default = 5 seconds)
$WAITING_TIME_FOR_RECONNECT = 1;		# duration of waiting for reconnect (default = 1 second)
$LogPREFIX = "";
$LogSUFFIX = "_PDC";
$SYSTEM = $ENV{'SYSTEMNAME'};			# System name used for selecting values from system_info.xml
$NUM_JOBS_FOR_WD_UPDATE = 100;					# number of jobs which are processed before wd update

# get date in format yyyymmdd_hhmi -> $dateTimeString
@timeStart = localtime(time);
$dateTimeString=($timeStart[5]+1900).(length($timeStart[4]+1)==1?"0".$timeStart[4]+1:$timeStart[4]+1).(length($timeStart[3])==1?"0".$timeStart[3]:$timeStart[3]);
$dateTimeString.="_".(length($timeStart[2])==1?"0".$timeStart[2]:$timeStart[2]).(length($timeStart[1])==1?"0".$timeStart[1]:$timeStart[1]);

$CONFIG_FILE = File::Spec->catfile("$ENV{'PMRootDir'}", "Security", "Passwords", "system_info.xml");
$LOGFILE = File::Spec->catfile("$ENV{'PMWorkflowLogDir'}", "BinLogs", "___Engine_@" . $SYSTEM . "@" . $ENGINE_ID ."@".$dateTimeString. ".log");
$PIDFILE = File::Spec->catfile("$ENV{'PMRootDir'}", "Bin", "Framework", "Engine_" . $SYSTEM . "@" . $ENGINE_ID . ".PID");
$DYNADIR = File::Spec->catfile("$ENV{'PMRootDir'}", "Security", "Comps");			# secured directory 
$ETL_CMD = File::Spec->catfile("$ENV{'INFA_HOME'}", "server", "bin", "pmcmd");
$LOGDIR = File::Spec->catfile("$ENV{'PMWorkflowLogDir'}", "ETLLogs");				# directory for logs 
$WORKDIR = File::Spec->catfile("$ENV{'PMRootDir'}", "Bin", "Framework");	# directory where PERL scripts are stored
$PERLEXE = "$ENV{'PERLEXE'}";			# PERL executable

$RUNNINGPL_JOB_CHECKS = 0;  # 0-false, 1-true
$RUNNINGPL_JOB_CHECKS_GJL_CYCLES = 10; #check is done after num. of GET_JOB_LIST_CYCLES 
$RUNNINGPL_JOB_CHECKS_JOBS_DURATION = 300; #job is check only if it's runnning more than specified duration [s]

#
#
################################################################################


################################################################################
################################################################################
##                                                                            ##
##   THERE IS NOTHING TO EDIT ON THE NEXT LINES !!!!!    DON'T DO IT !!!!!    ##
##                                                                            ##
################################################################################
################################################################################


################################################################################
#
#
# DO NOT CHANGE NEXT SETTINGS !!!

#
#
################################################################################

if($RUNNINGPL_JOB_CHECKS==1 && (uc($^O) eq "MSWIN32")) {
	require Win32::Process::List;
}

################################################################################
################################################################################
#                                                                              #
#                                   S U B S                                    #
#                                                                              #
################################################################################
################################################################################


################################################################################
# SUB write_log - writting message into LOGFILE file
################################################################################
sub write_log {
	@time = localtime(time);
	$year = 1900 + $time[5];
	$month = 1 + $time[4];
	printf LOG "%4d-%02d-%02d %02d:%02d:%02d  ", $year, $month, $time[3], $time[2], $time[1], $time[0];
	print LOG @_;
	printf "%4d-%02d-%02d %02d:%02d:%02d  ", $year, $month, $time[3], $time[2], $time[1], $time[0];
	print @_;
	return 0;
}

################################################################################
# Function remove current PID from PID file
################################################################################
sub remove_my_PID {
	my $pid;
	if (-f $PIDFILE) {
		open (IN,"<", "$PIDFILE");
		while(my $row = <IN>) {
			chomp($row);
        		$pid = $row;
		}
		close IN;
	}
	if ($pid && $pid==$$){
		open (OUT,">", "$PIDFILE");
		print OUT "";
		($DEBUG > 4) and write_log "DEBUG> Remove current PID from PID file.";
		close OUT;
	}else{
		($DEBUG > 4) and write_log "DEBUG> Don't remove PID from PID file. There is PID of another process or is missing.";
	}
}

################################################################################
# SUB report_error - writting message into LOGFILE file and exit
################################################################################
sub report_error {
	write_log "\n";
	write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
	write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
	write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
	write_log "\n";
	write_log @_;
	write_log "\n";
	write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
	write_log "ERROR> !!!                          E  X  I  T                               !!!\n";
	write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
	
	remove_my_PID;
	close LOG;
	
	exit 1;
}

################################################################################
# Function connect_Oracle - Oracle connect
################################################################################
sub connect_Oracle {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: connect_Oracle - START\n";
	$TABS = $TABS . "    ";

	$config = XMLin($CONFIG_FILE);
	$PDCserver = $config->{connection}->{$SYSTEM}->{PDCserver};
	$PDCuser = $config->{connection}->{$SYSTEM}->{PDCuser};
	$PDCpassword = $config->{connection}->{$SYSTEM}->{PDCpassword};
	$PDCserverODBC = $config->{connection}->{$SYSTEM}->{PDCserverODBC};
	
	$DS_DS_PATH=$config->{connection}->{$SYSTEM}->{DS_DS_PATH};
	$DS_ProdPath=$config->{connection}->{$SYSTEM}->{DS_ProdPath};
	$DS_ProdDSPath=$config->{connection}->{$SYSTEM}->{DS_ProdDSPath};
	$DS_TmpPath=$config->{connection}->{$SYSTEM}->{DS_TmpPath};
	$DS_DSProjName=$config->{connection}->{$SYSTEM}->{DS_DSProjName}; 
	$DS_SourceDataDir=$config->{connection}->{$SYSTEM}->{DS_SourceDataDir}; 
	
	$TD_DB_Pwd=$config->{connection}->{$SYSTEM}->{TD_DB_Pwd}; 
	$TD_DB_User=$config->{connection}->{$SYSTEM}->{TD_DB_User}; 
	$TD_DSN=$config->{connection}->{$SYSTEM}->{TD_DSN}; 
	$TD_MON_USER=$config->{connection}->{$SYSTEM}->{TD_MON_USER}; 
	$TD_MON_PASSWD=$config->{connection}->{$SYSTEM}->{TD_MON_PASSWD}; 
	$TD_MON_DB=$config->{connection}->{$SYSTEM}->{TD_MON_DB}; 
	$TD_ST_DB=$config->{connection}->{$SYSTEM}->{TD_ST_DB}; 
	$TD_TA_DB=$config->{connection}->{$SYSTEM}->{TD_TA_DB}; 
	$TD_WK_DB=$config->{connection}->{$SYSTEM}->{TD_WK_DB}; 
	
	$OCI_Inst_RGO=$config->{connection}->{$SYSTEM}->{OCI_Inst_RGO}; 
	$OCI_Pwd_RGO=$config->{connection}->{$SYSTEM}->{OCI_Pwd_RGO}; 
	$OCI_User_RGO=$config->{connection}->{$SYSTEM}->{OCI_User_RGO}; 
	$OCI_Inst=$config->{connection}->{$SYSTEM}->{OCI_Inst}; 
	$OCI_Pwd=$config->{connection}->{$SYSTEM}->{OCI_Pwd}; 
	$OCI_User=$config->{connection}->{$SYSTEM}->{OCI_User}; 
	
	$SMTP_From=$config->{connection}->{$SYSTEM}->{SMTP_From}; 
	$REP_MAIL=$config->{connection}->{$SYSTEM}->{REP_MAIL}; 
	
	$MSTRCmdMgrPath=$config->{connection}->{$SYSTEM}->{MSTRCmdMgrPath}; 
	$MSTRProj1=$config->{connection}->{$SYSTEM}->{MSTRProj1}; 
	$MSTRProj2=$config->{connection}->{$SYSTEM}->{MSTRProj2}; 
	$MSTRUsr=$config->{connection}->{$SYSTEM}->{MSTRUsr}; 
	$MSTRPwd=$config->{connection}->{$SYSTEM}->{MSTRPwd}; 

	$DeletePeriod=$config->{connection}->{$SYSTEM}->{DeletePeriod}; 
	$ADMIN_path=$config->{connection}->{$SYSTEM}->{ADMIN_path}; 
	$BKPPointPath=$config->{connection}->{$SYSTEM}->{BKPPointPath}; 
	$TPT_log_cil=$config->{connection}->{$SYSTEM}->{TPT_log_cil}; 	

	$ETLuser = $config->{connection}->{$SYSTEM}->{INFAuser};
	$ETLpassword = $config->{connection}->{$SYSTEM}->{INFApassword};
	$ETLdomain = $config->{connection}->{$SYSTEM}->{INFAdomain};
	$ETLintegration_service = $config->{connection}->{$SYSTEM}->{INFAintegration_service};
	
	$ETLODSuser = $config->{connection}->{$SYSTEM}->{INFAODSuser};
	$ETLODSpassword = $config->{connection}->{$SYSTEM}->{INFAODSpassword};
	$ETLODSdomain = $config->{connection}->{$SYSTEM}->{INFAODSdomain};
	$ETLODSintegration_service = $config->{connection}->{$SYSTEM}->{INFAODSintegration_service};
	
	($DEBUG > 4) and write_log "CHECK>              PDCserver: $PDCserver\n";
	($DEBUG > 4) and write_log "CHECK>                PDCuser: $PDCuser\n";
	($DEBUG > 8) and write_log "SECUR>            PDCpassword: $PDCpassword\n";
	($DEBUG > 4) and write_log "SECUR>          PDCserverODBC: $PDCserverODBC\n";	

#	($DEBUG > 4) and write_log "CHECK>              ETLdomain: $ETLdomain\n";
#	($DEBUG > 4) and write_log "CHECK> ETLintegration_service: $ETLintegration_service\n";
#	($DEBUG > 4) and write_log "CHECK>                ETLuser: $ETLuser\n";
#	($DEBUG > 8) and write_log "SECUR>            ETLpassword: $ETLpassword\n";
	
#	($DEBUG > 4) and write_log "CHECK>              ETLODSdomain: $ETLODSdomain\n";
#	($DEBUG > 4) and write_log "CHECK> ETLODSintegration_service: $ETLODSintegration_service\n";
#	($DEBUG > 4) and write_log "CHECK>                ETLODSuser: $ETLODSuser\n";
#	($DEBUG > 8) and write_log "SECUR>            ETLODSpassword: $ETLODSpassword\n";	

#	($DEBUG > 4) and write_log "CHECK>             DS_DS_PATH: $DS_DS_PATH\n";
#	($DEBUG > 4) and write_log "CHECK>            DS_ProdPath: $DS_ProdPath\n";
#	($DEBUG > 4) and write_log "CHECK>          DS_ProdDSPath: $DS_ProdDSPath\n";
#	($DEBUG > 4) and write_log "CHECK>             DS_TmpPath: $DS_TmpPath\n";
#	($DEBUG > 4) and write_log "CHECK>          DS_DSProjName: $DS_DSProjName\n";
#	($DEBUG > 4) and write_log "CHECK>       DS_SourceDataDir: $DS_SourceDataDir\n";

	
	my $max_try = 100;
	my $success = 0;
	my $n_try = 0;
	while (not $success and $n_try < $max_try) {
		$success = ($dbh=DBI->connect( $PDCserver, $PDCuser, $PDCpassword ))?1:0; 
		if(not $success) {
			report_warning( "ERROR> Couldn't connect to database:\n" . $DBI::errstr . "\n" );
			sleep $DELAY_AFTER_ERROR;
		}
	}
	if(not $success){
		report_error( "ERROR> Couldn't connect to database:\n" . $DBI::errstr . "\n" );
	}
	
	
	$dbh->{AutoCommit}    = 0;
	$dbh->{RaiseError}    = 1;
	$dbh->{ora_check_sql} = 0;
	$dbh->{RowCacheSize}  = 16;
	
	$dbh->do("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
	#$dbh->do("ALTER SESSION SET TIME_ZONE='0:0'");

	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: connect_Oracle - END\n";
	return $step;
}

################################################################################
# Function SP_ENG_TAKE_CONTROL - trying to take control in GET_JOB_LIST
################################################################################
sub SP_ENG_TAKE_CONTROL {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: SP_ENG_TAKE_CONTROL - START\n";
	$TABS = $TABS . "    ";

	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_TAKE_CONTROL(:engine_id_in, :system_name_in, :debug_in, :return_value_out, :return_status_out, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";
	my $success = 0;
	my $max_try = 3;
	my $n_try = 0;
	while (not $success and $n_try < $max_try) {
		($DEBUG > 4) and write_log "\n";
		($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
		($DEBUG > 4) and write_log "CHECK>      engine_id_in: $ENGINE_ID\n";
		($DEBUG > 4) and write_log "CHECK>    system_name_in: $SYSTEM\n";
		($DEBUG > 4) and write_log "CHECK>          debug_in: $DEBUGFLAG\n";
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
		$sth->bind_param_inout( ":engine_id_in", \$ENGINE_ID, 20);
		$sth->bind_param_inout( ":system_name_in", \$SYSTEM, 16);
		$sth->bind_param_inout( ":return_value_out", \$wait, 20);
		$sth->bind_param_inout( ":return_status_out", \$step, 10000);
		$sth->bind_param_inout( ":debug_in", \$DEBUGFLAG, 20);
		$sth->bind_param_inout( ":exit_cd", \$exit_cd, 20);
		$sth->bind_param_inout( ":errmsg_out", \$errmsg, 10000);
		$sth->bind_param_inout( ":errcode_out", \$errcode, 20);
		$sth->bind_param_inout( ":errline_out", \$errline, 10000);
		$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
		if ($exit_cd == 0) {
			($DEBUG > 4) and write_log "CHECK>  return_value_out: $wait\n";
			($DEBUG > 4) and write_log "CHECK> return_status_out: $step\n";
			($DEBUG > 4) and write_log "CHECK>           exit_cd: $exit_cd\n";
			($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
			($DEBUG > 4) and write_log "CHECK> Statement executed successfully\n";
			if ($exit_cd == 0) {
				$success = 1;
			}
			else {
				write_log "\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "ERROR> !!!                           E R R O R                               !!!\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "\n";
				write_log "ERROR> !!! Procedure returned exit_cd: \'$exit_cd\', repeating\n";
				write_log "\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "ERROR> !!!                           E R R O R                               !!!\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "\n";
				$dbh->disconnect if defined($dbh);
				sleep $DELAY_AFTER_ERROR;
				connect_Oracle;
			}
		}
		else {
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			write_log "ERROR> Exceuting of procedure has failed\n";
			write_log "ERROR> Error_message:\n$errmsg\n";
			write_log "ERROR> Error_code:$errcode\n";
			write_log "ERROR> Error_line:\n$errline\n";
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			$dbh->disconnect if defined($dbh);
			sleep $DELAY_AFTER_ERROR;
			connect_Oracle;
		}
		$n_try++;
	}
	($DEBUG > 4) and write_log "CHECK> WAIT: $wait\n";
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: SP_ENG_TAKE_CONTROL - END\n";
	return $wait;
}

################################################################################
# Function SP_ENG_GIVE_CONTROL - giving control in GET_JOB_LIST
################################################################################
sub SP_ENG_GIVE_CONTROL {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: SP_ENG_GIVE_CONTROL - START\n";
	$TABS = $TABS . "    ";

	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_GIVE_CONTROL(:engine_id_in, :system_name_in, :debug_in, :return_status_out, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";
	my $success = 0;
	my $max_try = 100;
	my $n_try = 0;
	while (not $success and $n_try < $max_try) {
		($DEBUG > 4) and write_log "\n";
		($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
		($DEBUG > 4) and write_log "CHECK>      engine_id_in: $ENGINE_ID\n";
		($DEBUG > 4) and write_log "CHECK>    system_name_in: $SYSTEM\n";
		($DEBUG > 4) and write_log "CHECK>          debug_in: $DEBUGFLAG\n";
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
		$sth->bind_param_inout( ":engine_id_in", \$ENGINE_ID, 20);
		$sth->bind_param_inout( ":system_name_in", \$SYSTEM, 16);
		$sth->bind_param_inout( ":return_status_out", \$step, 10000);
		$sth->bind_param_inout( ":debug_in", \$DEBUGFLAG, 20);
		$sth->bind_param_inout( ":exit_cd", \$exit_cd, 20);
		$sth->bind_param_inout( ":errmsg_out", \$errmsg, 10000);
		$sth->bind_param_inout( ":errcode_out", \$errcode, 20);
		$sth->bind_param_inout( ":errline_out", \$errline, 10000);
		$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
		if ($exit_cd == 0) {
			($DEBUG > 4) and write_log "CHECK> return_status_out: $step\n";
			($DEBUG > 4) and write_log "CHECK>           exit_cd: $exit_cd\n";
			($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
			($DEBUG > 4) and write_log "CHECK> Statement executed successfully\n";
			if ($exit_cd == 0) {
				$success = 1;
			}
			else {
				write_log "\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "ERROR> !!!                           E R R O R                               !!!\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "\n";
				write_log "ERROR> !!! Procedure returned exit_cd: \'$exit_cd\', repeating\n";
				write_log "\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "ERROR> !!!                           E R R O R                               !!!\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "\n";
				$dbh->disconnect if defined($dbh);
				sleep $DELAY_AFTER_ERROR;
				connect_Oracle;
			}
		}
		else {
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			write_log "ERROR> Exceuting of procedure has failed\n";
			write_log "ERROR> Error_message:\n$errmsg\n";
			write_log "ERROR> Error_code:$errcode\n";
			write_log "ERROR> Error_line:\n$errline\n";
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			$dbh->disconnect if defined($dbh);
			sleep $DELAY_AFTER_ERROR;
			connect_Oracle;
		}
		$n_try++;
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: SP_ENG_GIVE_CONTROL - END\n";
}

################################################################################
# Function SP_ENG_UPDATE_STATUS - updating of the job status
################################################################################
sub SP_ENG_UPDATE_STATUS {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: SP_ENG_UPDATE_STATUS - START\n";
	$TABS = $TABS . "    ";
	my $request = shift;
	my $launch = shift;

	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_UPDATE_STATUS(:job_id, :launch_id, :signal_in, :request_in, :engine_id_in, :system_name_in, :queue_number_in, :debug_in, :return_status_out, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";
	my $signal = "N/A";
	my $success = 0;
	my $max_try = 100;
	my $n_try = 0;
	while (not $success and $n_try < $max_try) {
		($DEBUG > 4) and write_log "\n";
		($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
		($DEBUG > 4) and write_log "CHECK>         job_id_in: $job_id\n";
		($DEBUG > 4) and write_log "CHECK>         launch_in: $launch\n";
		($DEBUG > 4) and write_log "CHECK>         signal_in: $signal\n";
		($DEBUG > 4) and write_log "CHECK>        request_in: $request\n";
		($DEBUG > 4) and write_log "CHECK>      engine_id_in: $ENGINE_ID\n";
		($DEBUG > 4) and write_log "CHECK>    system_name_in: $SYSTEM\n";
		($DEBUG > 4) and write_log "CHECK>   queue_number_in: $queue_number\n";
		($DEBUG > 4) and write_log "CHECK>          debug_in: $DEBUGFLAG\n";
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
		$sth->bind_param_inout( ":job_id", \$job_id, 20);
		$sth->bind_param_inout( ":launch_id", \$launch, 20);
		$sth->bind_param_inout( ":signal_in", \$signal, 16);
		$sth->bind_param_inout( ":request_in", \$request, 16);
		$sth->bind_param_inout( ":engine_id_in", \$ENGINE_ID, 20);
		$sth->bind_param_inout( ":system_name_in", \$SYSTEM, 20);
		$sth->bind_param_inout( ":queue_number_in", \$queue_number, 20);
		$sth->bind_param_inout( ":return_status_out", \$step, 10000);
		$sth->bind_param_inout( ":debug_in", \$DEBUGFLAG, 20);
		$sth->bind_param_inout( ":exit_cd", \$exit_cd, 20);
		$sth->bind_param_inout( ":errmsg_out", \$errmsg, 10000);
		$sth->bind_param_inout( ":errcode_out", \$errcode, 20);
		$sth->bind_param_inout( ":errline_out", \$errline, 10000);
		$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
		if ($exit_cd == 0) {
			($DEBUG > 4) and write_log "CHECK> return_status_out: $step\n";
			($DEBUG > 4) and write_log "CHECK>           exit_cd: $exit_cd\n";
			($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
			($DEBUG > 4) and write_log "CHECK> Statement executed successfully\n";
			if (uc($step) eq "RUN" or uc($step) eq "SKIP") {
				$success = 1;
			}
			else {
				write_log "\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "ERROR> !!!                           E R R O R                               !!!\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "\n";
				write_log "ERROR> !!! Procedure returned \'$step\' in return_status_out parameter, repeating\n";
				write_log "\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "ERROR> !!!                           E R R O R                               !!!\n";
				write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				write_log "\n";
				$dbh->disconnect if defined($dbh);
				sleep $DELAY_AFTER_ERROR;
				connect_Oracle;
			}
		}
		else {
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			write_log "ERROR> Exceuting of procedure has failed\n";
			write_log "ERROR> Error_message:\n$errmsg\n";
			write_log "ERROR> Error_code:$errcode\n";
			write_log "ERROR> Error_line:\n$errline\n";
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			$dbh->disconnect if defined($dbh);
			sleep $DELAY_AFTER_ERROR;
			connect_Oracle;
		}
		$n_try++;
	}
	($DEBUG > 4) and write_log "CHECK> job_id: $job_id  action: $step\n";
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: SP_ENG_UPDATE_STATUS - END\n";
	return $step;
}

################################################################################
# Function SP_ENG_GET_JOB_LIST - getting job list for execution
################################################################################
sub SP_ENG_GET_JOB_LIST {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: SP_ENG_GET_JOB_LIST - START\n";
	$TABS = $TABS . "    ";
	$success = 0;
	$exit_cd = 0;
	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_GET_JOB_LIST(:engine_id_in, :system_name_in, :debug_in, :csr, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";

	$job_list = ();
	while (not $success) {
		($DEBUG > 4) and write_log "\n";
		($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
		($DEBUG > 4) and write_log "CHECK>         ENGINE_ID: $ENGINE_ID\n";
		($DEBUG > 4) and write_log "CHECK>          debug_in: $DEBUGFLAG\n";
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
		$sth->bind_param_inout( ":engine_id_in", \$ENGINE_ID, 20);
		$sth->bind_param_inout( ":system_name_in", \$SYSTEM, 20);
		$sth->bind_param_inout( ":csr", \$csr, 0, { ora_type => ORA_RSET } );
		$sth->bind_param_inout( ":debug_in", \$DEBUGFLAG, 20);
		$sth->bind_param_inout( ":exit_cd", \$exit_cd, 20);
		$sth->bind_param_inout( ":errmsg_out", \$errmsg, 10000);
		$sth->bind_param_inout( ":errcode_out", \$errcode, 20);
		$sth->bind_param_inout( ":errline_out", \$errline, 10000);
		$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
		if ($exit_cd == 0) {
			$success = 1;
			($DEBUG > 4) and write_log "CHECK>           exit_cd: $exit_cd\n";
			($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
			($DEBUG > 4) and write_log "CHECK> Statement executed successfully\n";
			$seqnum = 0;
			while ( ($job_id, $job_name, $availability, $job_type, $job_category, $cmd_line, $queue_number, $load_date, $engine_id, $retention_period)=$csr->fetchrow_array()) {
				$job_list->{$seqnum}->{'job_id'} = $job_id;
				$job_list->{$seqnum}->{'job_name'} = $job_name;
				$job_list->{$seqnum}->{'availability'} = $availability;
				$job_list->{$seqnum}->{'job_type'} = $job_type;
				$job_list->{$seqnum}->{'job_category'} = $job_category;
				$job_list->{$seqnum}->{'cmd_line'} = $cmd_line;
				$job_list->{$seqnum}->{'queue_number'} = $queue_number;
				$job_list->{$seqnum}->{'load_date'} = $load_date;
				$job_list->{$seqnum}->{'engine_id'} = $engine_id;
				$job_list->{$seqnum}->{'retention_period'} = $retention_period;
				$seqnum++;
			}
		}
		else {
			$dbh->disconnect if defined($dbh);
			$NUM_CYCLE = 1;
			$NUM_CYCLE_ERR++;
			$NUM_CYCLE_TURN++;
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> Exceuting of procedure has failed\n";
			write_log "ERROR> Error_message:\n$errmsg\n";
			write_log "ERROR> Error_code:$errcode\n";
			write_log "ERROR> Error_line:\n$errline\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			sleep $DELAY_AFTER_ERROR;
			connect_Oracle;
		}
		if ($NUM_CYCLE++ % $MAX_CYCLE == 0) {
			$dbh->disconnect if defined($dbh);
			sleep $WAITING_TIME_FOR_RECONNECT;
			$NUM_CYCLE = 1;
			$NUM_CYCLE_TURN++;
			connect_Oracle;
		}

	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: SP_ENG_GET_JOB_LIST - END\n";
}

################################################################################
# Function SP_ENG_UPDATE_WD_STATUS - writting timestamp of last Engine round
################################################################################
sub SP_ENG_UPDATE_WD_STATUS {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: SP_ENG_UPDATE_WD_STATUS - START\n";
	$TABS = $TABS . "    ";

	$success = 0;
	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_UPDATE_WD_STATUS(:engine_id_in, :system_name_in, :debug_in, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";

	#$job_list = ();
	while (not $success) {
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
		$sth->bind_param_inout( ":engine_id_in", \$ENGINE_ID, 20);
		$sth->bind_param_inout( ":system_name_in", \$SYSTEM, 20);
		$sth->bind_param_inout( ":debug_in", \$DEBUGFLAG, 20);
		$sth->bind_param_inout( ":exit_cd", \$exit_cd, 20);
		$sth->bind_param_inout( ":errmsg_out", \$errmsg, 10000);
		$sth->bind_param_inout( ":errcode_out", \$errcode, 20);
		$sth->bind_param_inout( ":errline_out", \$errline, 10000);
		$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
		if ($exit_cd == 0) {
			($DEBUG > 4) and write_log "CHECK> Statement executed successfully\n";
			$success = 1;
		}
		else {
			$dbh->disconnect if defined($dbh);
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> Exceuting of procedure has failed\n";
			write_log "ERROR> Error_message:\n$errmsg\n";
			write_log "ERROR> Error_code:$errcode\n";
			write_log "ERROR> Error_line:\n$errline\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			sleep $DELAY_AFTER_ERROR;
			connect_Oracle;
		}
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: SP_ENG_UPDATE_WD_STATUS - END\n";
}

################################################################################
# Function sleeping - waiting before asking of the new job list in case that nothing has been returned in the last round
################################################################################
sub sleeping {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: sleeping - START\n";
	$TABS = $TABS . "    ";
	my $availability = shift;
	my $n_waiting = shift;
	if($availability == 8) {
		if($n_waiting > 9) {
			$n_waiting = 0;
			($DEBUG > 1) and write_log " STEP> =========================================================================\n";
			($DEBUG > 1) and write_log " STEP> ***  Sleeping ...\n";
			($DEBUG > 1) and write_log " STEP> =========================================================================\n";
			sleep $SLEEPING_TIME;
		}
		$n_waiting++;
		close LOG;
		open LOG, ">>$LOGFILE";
	}
    	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: sleeping - END\n";
	return $n_waiting;
}

################################################################################
# Function old_Engine_test - testing if old Engine is not running yet
################################################################################
sub old_Engine_test {
	($DEBUG > 1) and print "$TABS>>>>>>>>>> SUB: old_Engine_test - START\n";
	$TABS = $TABS . "    ";
	connect_Oracle;

	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_CHECK_WD_STATUS(:engine_id_in, :system_name_in, :debug_in, :number_of_seconds_out, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and print "  SQL>\n$SQL\n";
	$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
	$sth->bind_param_inout( ":engine_id_in", \$ENGINE_ID, 20);
	$sth->bind_param_inout( ":system_name_in", \$SYSTEM, 20);
	$sth->bind_param_inout( ":number_of_seconds_out", \$number_of_seconds, 0);
	$sth->bind_param_inout( ":debug_in", \$DEBUGFLAG, 20);
	$sth->bind_param_inout( ":exit_cd", \$exit_cd, 20);
	$sth->bind_param_inout( ":errmsg_out", \$errmsg, 10000);
	$sth->bind_param_inout( ":errcode_out", \$errcode, 20);
	$sth->bind_param_inout( ":errline_out", \$errline, 10000);
	$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;

	if ($exit_cd != 0) {
		$number_of_seconds = -999;
	}
	($DEBUG > 2) and print "DEBUG>           exit_cd: $exit_cd\n";
	($DEBUG > 2) and print "DEBUG> number_of_seconds: $number_of_seconds\n";

	if($number_of_seconds == 0) {
		$dbh->disconnect if defined($dbh);
		print "\n";
		print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
		print "Probably other Engine is still running, exiting...\n";
		print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
		print "\n";
		remove_my_PID;
		close LOG;
		exit 0; # od posledni obratky Engine neuplynulo jeste WATCHDOG_INTERVAL vterin
	}
    	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and print "$TABS<<<<<<<<<< SUB: old_Engine_test - END\n";
}

################################################################################
# Function take_control - new Wngine takes control after the old one
################################################################################
sub take_control {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: take_control - START\n";
	$TABS = $TABS . "    ";
	my $pid;
	if (-f $PIDFILE) {
		open IN, "<$PIDFILE";
		while(my $row = <IN>) {
			chomp($row);
        		$pid = $row;
		}
		if($pid){
			($DEBUG > 0) and print "\n";
			($DEBUG > 0) and print " INFO> +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
			($DEBUG > 0) and print " INFO>                         Killing process $pid\n";
			($DEBUG > 0) and print " INFO> +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
			($DEBUG > 0) and print "\n";
			if (uc($^O) eq "MSWIN32") {
				Win32::Process::KillProcess($pid, 1);
			}
			elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
				system("kill -15 $pid");
			}
		}
		close IN;
	}
	open OUT, ">$PIDFILE";
	print OUT "$$";
	close OUT;

	open LOG, ">>$LOGFILE";

	write_log "\n";
	write_log "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
	write_log "New Engine continue, PID = $$\n";
	write_log "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
	write_log "\n";

    $TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: take_control - END\n";
	
	close LOG;
}

################################################################################
# Function format_string - string formating
################################################################################
sub format_string {
	my $string = shift;
	my $len = shift;
	$string = substr($string,0,$len);
	my $len_act = length($string);

	for ($i=$len_act; $i < $len; $i++) {
		$string = $string . " ";
	}
	return $string;
}

################################################################################
# Function get_jobs_to_check - getting Running.pl PIDs to check
################################################################################
sub get_jobs_to_check {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: get_jobs_to_check - START\n";
	$TABS = $TABS . "    ";
	$success = 0;
	$exit_cd = 0;
	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_GET_JOBS_TO_RUN_CHECK(:engine_id_in, :system_name_in, :run_in_secs_in, :debug_in, :csr, :exit_cd_out, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";

	my %jobpid_list = ();
	while (not $success) {
		($DEBUG > 4) and write_log "\n";
		($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
		($DEBUG > 4) and write_log "CHECK>         ENGINE_ID: $ENGINE_ID\n";
		($DEBUG > 4) and write_log "CHECK>          debug_in: $DEBUGFLAG\n";
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
		$sth->bind_param_inout( ":engine_id_in", \$ENGINE_ID, 20);
		$sth->bind_param_inout( ":system_name_in", \$SYSTEM, 20);
		$sth->bind_param_inout( ":run_in_secs_in", \$RUNNINGPL_JOB_CHECKS_JOBS_DURATION, 20);
		$sth->bind_param_inout( ":csr", \$csr, 0, { ora_type => ORA_RSET } );
		$sth->bind_param_inout( ":debug_in", \$DEBUGFLAG, 20);
		$sth->bind_param_inout( ":exit_cd_out", \$exit_cd, 20);
		$sth->bind_param_inout( ":errmsg_out", \$errmsg, 10000);
		$sth->bind_param_inout( ":errcode_out", \$errcode, 20);
		$sth->bind_param_inout( ":errline_out", \$errline, 10000);
		$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
		if ($exit_cd == 0) {
			$success = 1;
			($DEBUG > 4) and write_log "CHECK>           exit_cd: $exit_cd\n";
			($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
			($DEBUG > 4) and write_log "CHECK> Statement executed successfully\n";

			while ( ($job_id,$queue_number, $running_job_pid)=$csr->fetchrow_array()) {
				$jobpid_list{$running_job_pid}{'job_id'} = $job_id;
				$jobpid_list{$running_job_pid}{'queue_number'} = $queue_number;
			}
		}
		else {
			$dbh->disconnect if defined($dbh);
			$NUM_CYCLE = 1;
			$NUM_CYCLE_ERR++;
			$NUM_CYCLE_TURN++;
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> Exceuting of procedure has failed\n";
			write_log "ERROR> Error_message:\n$errmsg\n";
			write_log "ERROR> Error_code:$errcode\n";
			write_log "ERROR> Error_line:\n$errline\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                           E R R O R                               !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			sleep $DELAY_AFTER_ERROR;
			connect_Oracle;
		}
		if ($NUM_CYCLE++ % $MAX_CYCLE == 0) {
			$dbh->disconnect if defined($dbh);
			sleep $WAITING_TIME_FOR_RECONNECT;
			$NUM_CYCLE = 1;
			$NUM_CYCLE_TURN++;
			connect_Oracle;
		}

	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: SP_ENG_GET_JOBS_TO_RUN_CHECK - END\n";
	return %jobpid_list;
}

################################################################################
# Function get_server_pids - get server PIDs
################################################################################
sub get_server_pids {
	my %list = ();
	if (uc($^O) eq "MSWIN32") {
		my $P = Win32::Process::List->new();
  		%list = $P->GetProcesses();      
  			
	}elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
		open(FILE, "ps -ef|");
		while (<FILE>)
		{
		($uid,$pid,$ppid,$c,$stime,$tty,$time,$cmd) = split;
		$list{$pid}=$cmd;
		}		
	}
	return %list;
}

################################################################################
# Function running_pl_jobs_check - check if running.pl are running, othewise failed them
################################################################################
sub running_pl_jobs_check{
		
		my %PIDs = get_server_pids;
		my %runJobs = get_jobs_to_check;
		
		foreach my $jobPID ( keys %runJobs){
			if(!exists($PIDs{$jobPID})){			
				$job_id = $runJobs{$jobPID}{'job_id'};
				$queue_number = $runJobs{$jobPID}{'queue_number'}; 

				write_log "\n";
				write_log "********************************************************************************\n";
				write_log "Running.pl - PID $jobPID is not running -> marking job_id=$runJobs{$jobPID}{'job_id'} as failed.\n";
				write_log "********************************************************************************\n";
				write_log "\n";

				SP_ENG_UPDATE_STATUS ("RJ_FAILED", 0); # request, launch
			}
		}
}

################################################################################
################################################################################
#                                                                              #
#                                   M A I N                                    #
#                                                                              #
################################################################################
################################################################################


# DEBUG = 0 - only success & errors
# DEBUG > 0 - only success & errors & INFO
# DEBUG > 1 - only success & errors & INFO & STEP
# DEBUG > 2 - only success & errors & INFO & STEP & DEBUG
# DEBUG > 3 - only success & errors & INFO & STEP & DEBUG & SQL
# DEBUG > 4 - only success & errors & INFO & STEP & DEBUG & SQL & CHECK
# DEBUG > 5 - only success & errors & INFO & STEP & DEBUG & SQL & CHECK & TRACE
# DEBUG > 8 - ALL (SECUR)

$| = 1; 					# vypnuti cache



if (uc($^O) eq "MSWIN32") {
		$FILEEXT = "bat";
}
elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
		$FILEEXT = "sh";
}

print "\n";
print "################################################################################\n";
print "################################################################################\n";
print "###                                                                          ###\n";
print "###                                 S T A R T                                ###\n";
print "###                                                                          ###\n";
print "################################################################################\n";
print "################################################################################\n";

$TABS = "";
$NUM_CYCLE = 0;
$NUM_CYCLE_ERR = 0;
$NUM_CYCLE_TURN = 0;
$RUNNINGPL_JOB_CHECKS_GJL_CYCLES_CNT=0;

($DEBUG > 5) and print "TRACE>                     SYSTEM: $SYSTEM\n";
($DEBUG > 5) and print "TRACE>              SLEPPING_TIME: $SLEEPING_TIME\n";
($DEBUG > 5) and print "TRACE>                  MAX_CYCLE: $MAX_CYCLE\n";
($DEBUG > 5) and print "TRACE>          DELAY_AFTER_ERROR: $DELAY_AFTER_ERROR\n";
($DEBUG > 5) and print "TRACE> WAITING_TIME_FOR_RECONNECT: $WAITING_TIME_FOR_RECONNECT\n";
($DEBUG > 5) and print "TRACE>                  LogPREFIX: $LogPREFIX\n";
($DEBUG > 5) and print "TRACE>                  LogSUFFIX: $LogSUFFIX\n";
($DEBUG > 5) and print "TRACE>                     SYSTEM: $SYSTEM\n";
($DEBUG > 5) and print "TRACE>                CONFIG_FILE: $CONFIG_FILE\n";
($DEBUG > 5) and print "TRACE>                    LOGFILE: $LOGFILE\n";
($DEBUG > 5) and print "TRACE>                    PIDFILE: $PIDFILE\n";
($DEBUG > 5) and print "TRACE>                    ETL_CMD: $ETL_CMD\n";
($DEBUG > 5) and print "TRACE>                    DYNADIR: $DYNADIR\n";

open LOG, ">>$LOGFILE";

old_Engine_test;				# test, zda nejede stary Engine

$ETL_START = "$ETL_CMD startworkflow -sv $ETLintegration_service -d $ETLdomain -u $ETLuser -p $ETLpassword -norecovery -wait -f ";
$ETL_ABORT = "$ETL_CMD abortworkflow -sv $ETLintegration_service -d $ETLdomain -u $ETLuser -p $ETLpassword -wait -f ";
$ETL_RESUME = "$ETL_CMD startworkflow -sv $ETLintegration_service -d $ETLdomain -u $ETLuser -p $ETLpassword -norecovery -wait -f ";
$ETL_START_NOWAIT = "$ETL_CMD startworkflow -sv $ETLintegration_service -d $ETLdomain -u $ETLuser -p $ETLpassword -norecovery -nowait -f ";

$ETLODS_START = "$ETL_CMD startworkflow -sv $ETLODSintegration_service -d $ETLODSdomain -u $ETLODSuser -p $ETLODSpassword -norecovery -wait -f ";
$ETLODS_ABORT = "$ETL_CMD abortworkflow -sv $ETLODSintegration_service -d $ETLODSdomain -u $ETLODSuser -p $ETLODSpassword -wait -f ";
$ETLODS_RESUME = "$ETL_CMD startworkflow -sv $ETLODSintegration_service -d $ETLODSdomain -u $ETLODSuser -p $ETLODSpassword -norecovery -wait -f ";

take_control;					# prevzeti rizeni novym Enginem

open LOG, ">>$LOGFILE";

($DEBUG > 2) and write_log "\n";
($DEBUG > 2) and write_log "DEBUG> =========================================================================\n";
($DEBUG > 2) and write_log "DEBUG> ===                  N a s t a v e n i   p a r a m e t r u           ====\n";
($DEBUG > 2) and write_log "DEBUG>         ENGINE_ID: $ENGINE_ID\n";
($DEBUG > 2) and write_log "DEBUG>           LOGFILE: $LOGFILE\n";
($DEBUG > 2) and write_log "DEBUG>           PIDFILE: $PIDFILE\n";
($DEBUG > 2) and write_log "DEBUG>             DEBUG: $DEBUG\n";
($DEBUG > 2) and write_log "DEBUG>      Oracle debug: $DEBUGFLAG\n";
($DEBUG > 2) and write_log "DEBUG>            -debug: $debugflag\n";
($DEBUG > 4) and write_log "CHECK>            SYSTEM: $SYSTEM\n";
($DEBUG > 4) and write_log "CHECK>       CONFIG_FILE: $CONFIG_FILE\n";
($DEBUG > 4) and write_log "CHECK>           DYNADIR: $DYNADIR\n";
($DEBUG > 4) and write_log "CHECK>         ETL_START: $ETL_START\n";
($DEBUG > 4) and write_log "CHECK>         ETL_ABORT: $ETL_ABORT\n";
($DEBUG > 4) and write_log "CHECK>        ETL_RESUME: $ETL_RESUME\n";
($DEBUG > 4) and write_log "CHECK>         ETLODS_START: $ETLODS_START\n";
($DEBUG > 4) and write_log "CHECK>         ETLODS_ABORT: $ETLODS_ABORT\n";
($DEBUG > 4) and write_log "CHECK>        ETLODS_RESUME: $ETLODS_RESUME\n";
($DEBUG > 2) and write_log "DEBUG> =========================================================================\n";

write_log "\n";
write_log "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
write_log "New Engine started\n";
write_log "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
write_log "\n";

################################################################################
# Hlavni cyklus
################################################################################

$availability = 0;  # pocatecni nastaveni
$n_waiting = 0;
$DEBUG and write_log "\n";
$DEBUG and write_log "\n";
$DEBUG and write_log "\n";
$DEBUG and write_log "\n";
$DEBUG and write_log "\n";
close LOG;

while ($availability != 9) {
	open LOG, ">>$LOGFILE";	
	$DEBUG and write_log " INFO>\n";
	$DEBUG and write_log " INFO> =========================================================================\n";
	$DEBUG and write_log " INFO> ===                 S T A R T   C Y C L E                             ===\n";
	$DEBUG and write_log " INFO> =========================================================================\n";
	$DEBUG and write_log " INFO>\n";

	if (!SP_ENG_TAKE_CONTROL) {

		SP_ENG_GET_JOB_LIST;

		($DEBUG > 2) and write_log "DEBUG>\n";
		($DEBUG > 2) and write_log "DEBUG> @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";
		($DEBUG > 2) and write_log "DEBUG> CYCLE_TURN: $NUM_CYCLE_TURN      CYCLE_NUM: $NUM_CYCLE      CYCLE_ERROR: $NUM_CYCLE_ERR\n";
		($DEBUG > 2) and write_log "DEBUG> @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";
		($DEBUG > 2) and write_log "DEBUG> Job_id|Job_name                  |A|Job_type        |Qu|Load_dt  |Cmd_lin\n";
		($DEBUG > 2) and write_log "DEBUG> -------------------------------------------------------------------------\n";
		#($DEBUG > 2) and write_log "DEBUG> 123456|12345678901234567890123456|1|1234567890123456|12|12.12.12|---------------\n";
		if ($DEBUG > 2) {
			for $seqnum (keys %$job_list) {
        			$_job_id = $job_list->{$seqnum}->{'job_id'};
				$_job_id = format_string($_job_id, 6);
				
        			$_job_name = $job_list->{$seqnum}->{'job_name'};
				$_job_name = format_string($_job_name, 26);

        			$_availability = $job_list->{$seqnum}->{'availability'};

        			$_job_type = $job_list->{$seqnum}->{'job_type'};
				$_job_type = format_string($_job_type, 16);

        			$_job_category = $job_list->{$seqnum}->{'job_category'};

				if (defined  $job_list->{$seqnum}->{'cmd_line'}) {
        				$_cmd_line = $job_list->{$seqnum}->{'cmd_line'};
				}
				else {
					$_cmd_line = "echo ON";
				}
	
        			$_queue_number = $job_list->{$seqnum}->{'queue_number'};
				$_queue_number = format_string($_queue_number, 2);

        			$_load_date = $job_list->{$seqnum}->{'load_date'};
				$_load_date = format_string($_load_date, 19);

				$_retention_period = $job_list->{$seqnum}->{'retention_period'};
				$_retention_period = format_string($_retention_period, 5);

				write_log "DEBUG> $_job_id|$_job_name|$_availability|$_job_type|$_queue_number|$_load_date|$_retention_period|$_cmd_line\n";			}
		}
		($DEBUG > 2) and write_log "DEBUG> @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";
		($DEBUG > 2) and write_log "DEBUG>\n";

		$n_jobs = 0;
		for $seqnum (keys %$job_list) {
        		$job_id = $job_list->{$seqnum}->{'job_id'};
        		$job_name = $job_list->{$seqnum}->{'job_name'};
        		$availability = $job_list->{$seqnum}->{'availability'};
        		$job_type = $job_list->{$seqnum}->{'job_type'};
        		$job_category = $job_list->{$seqnum}->{'job_category'};
			if (defined  $job_list->{$seqnum}->{'cmd_line'}) {
        			$cmd_line = $job_list->{$seqnum}->{'cmd_line'};
			}
			else {
				$cmd_line = "echo ON";
			}
        		$queue_number = $job_list->{$seqnum}->{'queue_number'};
        		$load_date = $job_list->{$seqnum}->{'load_date'};
				$DS_load_date = format_string($load_date,10);
				$DS_load_date =~ s/-//g ;
				$retention_period= $job_list->{$seqnum}->{'retention_period'};
			$cmd_line =~ s/\s*[>\x23\n].*//s; # odrizni vse do prvniho ">", hash, nebo \n dal (vcetne predchazejicich mezer) - odstraneni presmerovani vystupu
			$cmd_line = $cmd_line . File::Spec->catfile(" \>\>$LOGDIR", ${LogPREFIX} . ${job_name} . "\@" . ${job_id} . ${LogSUFFIX} . ".log 2\>\&1"); # dosad jednotne presmerovani vystupu
			if ($availability != 8 and $availability != 9) {
				($DEBUG > 4) and write_log "\n";
				($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
				($DEBUG > 4) and write_log "CHECK> ~~~                J o b   p a r a m e t e r s                        ~~~\n";
				($DEBUG > 4) and write_log                                                   "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
				($DEBUG > 4) and ($availability == 1) and ($queue_number >= 0) and write_log "CHECK>    job#: ##########   $n_jobs   ##########            R U N   J O B - START\n";
				($DEBUG > 4) and ($availability == 2) and ($queue_number >= 0) and write_log "CHECK>    job#: ##########   $n_jobs   ##########          R U N   J O B - RESTART\n";
				($DEBUG > 4) and ($availability == 3) and ($queue_number >= 0) and write_log "CHECK>    job#: ##########   $n_jobs   ##########           R U N   J O B - RESUME\n";
				($DEBUG > 4) and ($queue_number < 0) and write_log                           "CHECK>    job#: ##########   $n_jobs   ##########                  S K I P   J O B\n";
				($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
				($DEBUG > 4) and write_log "CHECK> availability: $availability\n";
				($DEBUG > 4) and write_log "CHECK>       job_id: $job_id\n";
				($DEBUG > 4) and write_log "CHECK>     job_name: $job_name\n";
				($DEBUG > 4) and write_log "CHECK>     job_type: $job_type\n";
				($DEBUG > 4) and write_log "CHECK> job_category: $job_category\n";
				($DEBUG > 4) and write_log "CHECK>     cmd_line: $cmd_line\n";
				($DEBUG > 4) and write_log "CHECK> queue_number: $queue_number\n";
				($DEBUG > 4) and write_log "CHECK>    load_date: $load_date\n";
				($DEBUG > 4) and write_log "CHECK> DS_load_date: $DS_load_date\n";
				($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
				($DEBUG > 4) and write_log "\n";

				$DEBUG and write_log " INFO> *************************************************************************\n";
				if ($queue_number < 0) {
					write_log "*****  Skipping job\# $n_jobs: ID=$job_id NM=$job_name AV=$availability JT=$job_type QN=$queue_number DT=$load_date\n";
				}
				else {
					write_log "*****  Starting job\# $n_jobs: ID=$job_id NM=$job_name AV=$availability JT=$job_type QN=$queue_number DT=$load_date\n";
				}
				$DEBUG and write_log " INFO> *************************************************************************\n";
				$n_jobs++;
				if ($availability == 1 or $availability == 2 or $availability == 3) { 
					if (uc($job_type) eq "DATASTAGE" or uc($job_type) eq "BTEQ" or uc($job_type) eq "DS_PERL") {
						$cmd = $cmd_line;
						$cmd =~ s/%DS_DS_PATH%/$DS_DS_PATH/gi ;
						$cmd =~ s/%DS_ProdPath%/$DS_ProdPath/gi ;
						$cmd =~ s/%DS_ProdDSPath%/$DS_ProdDSPath/gi ;
						$cmd =~ s/%DS_TmpPath%/$DS_TmpPath/gi ;
						$cmd =~ s/%DS_DSProjName%/$DS_DSProjName/gi ;
						$cmd =~ s/%BusinessDate%/$DS_load_date/gi ;
						$cmd =~ s/%DS_SourceDataDir%/$DS_SourceDataDir/gi ;
						$cmd =~ s/%PDCserver%/$PDCserver/gi ;
						$cmd =~ s/%PDCserverODBC%/$PDCserverODBC/gi ;
						$cmd =~ s/%PDCuser%/$PDCuser/gi;	
						$cmd =~ s/%PDCpassword%/$PDCpassword/gi ;	
						$cmd =~ s/%TD_DB_Pwd%/$TD_DB_Pwd/gi ;
						$cmd =~ s/%TD_DB_User%/$TD_DB_User/gi ;
						$cmd =~ s/%TD_DSN%/$TD_DSN/gi ;
						$cmd =~ s/%TD_MON_USER%/$TD_MON_USER/gi ;
						$cmd =~ s/%TD_MON_PASSWD%/$TD_MON_PASSWD/gi ;
						$cmd =~ s/%TD_MON_DB%/$TD_MON_DB/gi ;
						$cmd =~ s/%TD_ST_DB%/$TD_ST_DB/gi ;
						$cmd =~ s/%TD_TA_DB%/$TD_TA_DB/gi ;
						$cmd =~ s/%TD_WK_DB%/$TD_WK_DB/gi ;
						$cmd =~ s/%OCI_Inst_RGO%/$OCI_Inst_RGO/gi ;
						$cmd =~ s/%OCI_Pwd_RGO%/$OCI_Pwd_RGO/gi ;
						$cmd =~ s/%OCI_User_RGO%/$OCI_User_RGO/gi ;
						$cmd =~ s/%OCI_Inst%/$OCI_Inst/gi ;
						$cmd =~ s/%OCI_Pwd%/$OCI_Pwd/gi ;
						$cmd =~ s/%OCI_User%/$OCI_User/gi ;
						$cmd =~ s/%MSTRCmdMgrPath%/$MSTRCmdMgrPath/gi ;
						$cmd =~ s/%MSTRProj1%/$MSTRProj1/gi ;
						$cmd =~ s/%MSTRProj2%/$MSTRProj2/gi ;
						$cmd =~ s/%MSTRUsr%/$MSTRUsr/gi ;
						$cmd =~ s/%MSTRPwd%/$MSTRPwd/gi ;	
						$cmd =~ s/%SMTP_From%/$SMTP_From/gi ;	
						$cmd =~ s/%REP_MAIL%/$REP_MAIL/gi ;
						$cmd =~ s/%DeletePeriod%/$DeletePeriod/gi ;
						$cmd =~ s/%ADMIN_path%/$ADMIN_path/gi ;
						$cmd =~ s/%BKPPointPath%/$BKPPointPath/gi ;
						$cmd =~ s/%TPT_log_cil%/$TPT_log_cil/gi ;						
						$cmd = $cmd . "\n";
					}
					elsif (uc($job_type) eq "ABORT_INFORMATICA_ODS") { 
						$cmd = "$ETLODS_ABORT " . $cmd_line . "\n";
					}
					elsif (uc($job_type) eq "INFORMATICA_NOWAIT") { 
						$cmd = "$ETL_START_NOWAIT " . $cmd_line . "\n";
					}
					elsif (uc($job_type) eq "DATA_QUALITY" or uc($job_type) eq "MAN_DATA_QUALITY") {
						$cmd = "$cmd_line -name $job_name\n";
					}
					elsif (uc($job_type) eq "CHECKER" 
						or uc($job_type) eq "DELIVERY_CHECKER"
						or uc($job_type) eq "EXPORT_TGT"
						or uc($job_type) eq "EXPORT_WRK"
						or uc($job_type) eq "HISTORIZATION"
						or uc($job_type) eq "LOADER_DM"  
						or uc($job_type) eq "LOADER_ERR"
						or uc($job_type) eq "LOADER_STG" 
						or uc($job_type) eq "LOADER_TGT" 
						or uc($job_type) eq "LOADER_WRK" 
						or uc($job_type) eq "SNIFFER"
						or uc($job_type) eq "TRANSFORMATION"  
						or uc($job_type) eq "UNIFICATION"              
						or uc($job_type) eq "VALIDATOR") {
						if ($availability == 1) { 			# START
							$cmd = "$ETL_START " . $cmd_line . "\n";
						}
						elsif ($availability == 2) {			# RESUME
							$cmd = "$ETL_RESUME " . $cmd_line . "\n";
						}
						elsif ($availability == 3) {			# RESTART
                    					$cmd = "$ETL_ABORT " . $cmd_line . "\n";
                    					$cmd = $cmd . "$ETL_START " . $cmd_line . "\n";
						}
					}
					elsif (uc($job_type) eq "COMMAND_ARG") {
						$cmd = $cmd_line . " -availability $availability";
						$cmd = $cmd . " -job_id $job_id";
						$cmd = $cmd . " -job_name $job_name";
						$cmd = $cmd . " -job_type $job_type";
						$cmd = $cmd . " -job_category $job_category";
						$cmd = $cmd . " -load_date \"$load_date\"";
						$cmd = $cmd . " -queue_number $queue_number ";
						$cmd = $cmd . " -engine_id $ENGINE_ID ";
						$cmd = $cmd . " -system_name $SYSTEM ";
						
					}
					elsif (uc($job_type) eq "RUN_SCRIPT") {
						$cmd = $ENV{'PERLEXE'} . ' ' . File::Spec->catfile("$ENV{'PMRootDir'}", "Cmd", "run_script.pl") . " $ENV{'PMRootDir'} $ENV{'PMWorkflowLogDir'} " . $cmd_line;
					}
					elsif (uc($job_type) eq "RUN_SCRIPT_JOB") {
						$cmd = $ENV{'PERLEXE'} . ' ' . File::Spec->catfile("$ENV{'PMRootDir'}", "Cmd", "run_script_job.pl") . "  $job_name $job_id \"$load_date\" " . $cmd_line;
					}					
					elsif (uc($job_type) eq "RUN_SCRIPT_INTERVAL") {
						$cmd = $ENV{'PERLEXE'} . ' ' . File::Spec->catfile("$ENV{'PMRootDir'}", "Cmd", "run_script.pl") . " $job_name $job_id \"$load_date\" $retention_period " . $cmd_line;
					}
					elsif (uc($job_type) eq "RUN_SCRIPT_COMPS_INTERVAL") {
						$cmd = $ENV{'PERLEXE'} . ' ' . File::Spec->catfile("$ENV{'PMRootDir'}", "Cmd", "run_script_comps.pl") . " $job_name $job_id \"$load_date\" $retention_period " . $cmd_line;
					}
					else {
						$cmd = $cmd_line;
					}
                		}

				if (uc($^O) eq "MSWIN32") {
					$cmd = "SET JOB_NAME=$job_name\n" . $cmd;  	# pripojeni (predrazeni) jména jobu
        				require Win32::Process;
				}
				elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
					#$cmd = "set JOB_NAME=$job_name\nexport JOB_NAME\n" . $cmd;  	# pripojeni (predrazeni) jména jobu
					$cmd = $cmd;
				}
				($DEBUG > 5) and write_log "TRACE> cmd: $cmd\n";
	
				if ($availability == 1 or $availability == 2 or $availability == 3) { 
					if ($queue_number != -1) {		# only for physicaly launchable jobs, not for skipped

						$BAT = File::Spec->catfile("$DYNADIR", "eng_" . ${ENGINE_ID} . "_job_" . ${queue_number} . "." . ${FILEEXT});
						($DEBUG > 4) and write_log "CHECK> BAT: $BAT\n";
						open BAT, ">$BAT" or  next;
						print BAT $cmd or next;
						close BAT or next;
						($DEBUG > 5) and write_log "TRACE> BAT file created. Let's check size.\n";
						if ((-s $BAT) > 0) {
							($DEBUG > 5) and write_log "TRACE> BAT file succesfully created\n";
						}else{
							($DEBUG > 5) and write_log "TRACE> BAT file is empty. Try next.\n";
							next;
						}
					}
					$step = SP_ENG_UPDATE_STATUS ("SUCCESS", 1); # request, launch
					($DEBUG > 4) and write_log "CHECK> Job action: $step\n";
					if ($step eq 'run') {
						$failed = 0;
						$programm = $PERLEXE;
						$arguments = $PERLEXE . " " . File::Spec->catfile($WORKDIR, "Prepare_job.pl -file $BAT -id $job_id -name $job_name -type $job_type -queue $queue_number -engine $ENGINE_ID -system $SYSTEM -x $DEBUG $debugflag");
						if (uc($^O) eq "MSWIN32") {
							($DEBUG > 5) and write_log "TRACE> Starting programm: $programm\n";
							($DEBUG > 5) and write_log "TRACE> Programm and arguments: $arguments\n";
							Win32::Process::Create($ProcessObj,
							"$programm",
							"$arguments",
							0,
							NORMAL_PRIORITY_CLASS,
							".") or $failed = 1;
	
							$ProcessObj->Wait(INFINITE);
							$ProcessObj->GetExitCode($exitcode);
						}
						elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
							($DEBUG > 5) and write_log "TRACE> Programm and arguments: $arguments\n";
							my $thr = threads->create(sub {system($arguments);});
							$exitcode = $thr->join();
						}
						($DEBUG > 5) and write_log "TRACE> Programm returned exit code = $exitcode\n";
						if ($exitcode == 259) {$exitcode = 0;}
						($DEBUG > 5) and write_log "TRACE> Programm returned failed = $failed\n";
						if ($exitcode != 0 or $failed) {
							write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
							write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
							write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
							write_log "\n";
							write_log "ERROR> !!! Programm start failed                                             !!!\n";
							write_log "\n";
							write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
							write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
							write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
							SP_ENG_UPDATE_STATUS ("FAILED", 0); # request, launch
						}
					}
					elsif ($step eq 'skip') {
						SP_ENG_UPDATE_STATUS ("SUCCESS", 0); # request, launch
						($DEBUG > 5) and write_log "TRACE> Skipping job finished\n";
					} 
					else {
						($DEBUG > 4) and write_log "CHECK> !!!!!!!!!!\n";
						($DEBUG > 4) and write_log "CHECK> !!! Problem occured, taking another job\n";
						($DEBUG > 4) and write_log "CHECK> !!!!!!!!!!\n";
						next;
					}
				}
			}
			if (($n_jobs % $NUM_JOBS_FOR_WD_UPDATE == 0) && $n_jobs>0 ){ 
				write_log "DEBUG> NUM_JOBS_FOR_WD_UPDATE limit reached.\n";
				SP_ENG_UPDATE_WD_STATUS;
			}
		}
        SP_ENG_UPDATE_WD_STATUS;
		SP_ENG_GIVE_CONTROL;

		$RUNNINGPL_JOB_CHECKS_GJL_CYCLES_CNT++;

		if(($RUNNINGPL_JOB_CHECKS_GJL_CYCLES_CNT % $RUNNINGPL_JOB_CHECKS_GJL_CYCLES==0) && $RUNNINGPL_JOB_CHECKS){
			running_pl_jobs_check;
		}

		if ($availability == 8) {
        	$n_waiting = sleeping $availability, $n_waiting;
		}
		sleep 1;
		close LOG;
	}
	else {	# unsuccessfully taked control, waiting for next try
		sleep 1;
	}
}

open LOG, ">>$LOGFILE";
write_log "\n";
write_log "********************************************************************************\n";
write_log "***                             F I N I S H                                  ***\n";
write_log "********************************************************************************\n";
write_log "\n";
write_log "Everything done, exiting...\n";
write_log "\n";
write_log "********************************************************************************\n";
write_log "***                             F I N I S H                                  ***\n";
write_log "********************************************************************************\n";
write_log "\n";
$dbh->disconnect if defined($dbh);

remove_my_PID;
close LOG;

exit 0;

