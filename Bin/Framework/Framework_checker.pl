#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  Name:		Framework_checker.pl
#  IN_parameters:	[-x debug_level] [-debug]
#  OUT_paramaters:	exit_cd
#  Called from:		Cron scheduler every five minutes
#  Calling:		SP_FWRK_* procedures
#-------------------------------------------------------------------------------
#  Project:		PDC
#  Author:		Teradata - Petr Stefanek
#  Date:		2011-12-13
#-------------------------------------------------------------------------------
#  Version:		1.0
#-------------------------------------------------------------------------------
#  Description:		Script checks jobs
#-------------------------------------------------------------------------------
#  Version:		1.1
#  Modified: 	Teradata - Milan Budka
#  Date:		2014-12-01
#  Modification: Engine_name to end of SNMP trap
#  Version:		1.2
#  Modified: 	Teradata - Milan Budka
#  Date:		2015-03-17
#  Modification: System_name to end of SNMP trap
#  Modified: 	Teradata - Milan Budka
#  Date:		2016-03-11
#  Modification: OID fix
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

# Script check framework. In case of issue, generates message
use Getopt::Long;
use DBI;
# We need DBD::Oracle data types for REF Cursor variable
use DBD::Oracle qw(:ora_types);
use XML::Simple;
use Sys::Hostname;
use File::Spec;

GetOptions(
	"x=i"=> \$DEBUG,
	"debug"=> \$DEBUGFLAG
	);
if (not defined $DEBUG) { $DEBUG = 0; }
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
($DEBUG > 4) and print "CHECK>     DEBUG: $DEBUG\n";

print "Unprocessed by Getopt::Long\n" if $ARGV[0];
foreach (@ARGV) {
	print "$_\n";
}

################################################################################
# Konfigurovatelne parametry
#

$SYSTEM = $ENV{"SYSTEMNAME"};			# jmeno PDC
$CONFIG_FILE = File::Spec->catfile("$ENV{'PMRootDir'}", "Security", "Passwords", "system_info.xml");
$LOGFILE = File::Spec->catfile("$ENV{'PMWorkflowLogDir'}", "BinLogs", "Framework_checker.log");
$DELAY_AFTER_ERROR = 5;
$MAX_ERRORS = 5;				# pocet chyb nez se ukonci proces
$SLEEP_AFTER_ERROR = 5;			# pocet vterin cekani po chybe
$OID_Parse_Suffix="1.3.6.1.4.1.191.103.0.2.1";
$IP_receiver="TCP:" . $ENV{'SNMP_DEST'};  # adresa SNMP receiveru
$PARAM="-m ALL -Ci -v 2c -c public ".$IP_receiver." \"\" ".$OID_Parse_Suffix." "; ### Ci je INFORM
$SNMPTRAP = File::Spec->catfile("I:","usr", "Bin", "snmptrap.exe");

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
	if(open LOG, ">>$LOGFILE"){
		printf LOG "%4d-%02d-%02d %02d:%02d:%02d  ", $year, $month, $time[3], $time[2], $time[1], $time[0];
		print LOG @_;
	       	close LOG;
	}
	printf "%4d-%02d-%02d %02d:%02d:%02d  ", $year, $month, $time[3], $time[2], $time[1], $time[0];
	print @_;
	return 0;
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
	exit 1;
}

################################################################################
# Funkce connect_Oracle - Oracle connect
################################################################################
sub connect_Oracle {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: connect_Oracle - START\n";
	$TABS = $TABS . "    ";

	$config = XMLin($CONFIG_FILE);
	$PDCserver = $config->{connection}->{$SYSTEM}->{PDCserver};
	$PDCuser = $config->{connection}->{$SYSTEM}->{PDCuser};
	$PDCpassword = $config->{connection}->{$SYSTEM}->{PDCpassword};
	($DEBUG > 4) and write_log "CHECK>   PDCserver: $PDCserver\n";
	($DEBUG > 4) and write_log "CHECK>     PDCuser: $PDCuser\n";
	($DEBUG > 8) and write_log "SECUR> PDCpassword: $PDCpassword\n";

	$dbh = DBI->connect( $PDCserver, $PDCuser, $PDCpassword ) || report_error( "ERROR> Couldn't connect to database:\n" . $DBI::errstr . "\n" );
	$dbh->{AutoCommit}    = 0;
	$dbh->{RaiseError}    = 1;
	$dbh->{ora_check_sql} = 0;
	$dbh->{RowCacheSize}  = 16;

	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: connect_Oracle - END\n";
}

################################################################################
# Funkce run_SQL_procedure - spusti Oracle proceduru
################################################################################
sub run_SQL_procedure {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: run_SQL_procedure - START\n";
	$TABS = $TABS . "    ";
	$success = 0;
	$package = shift;
	$procedure = shift;
	($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
	($DEBUG > 4) and write_log "CHECK> Procedure name: $procedure\n";
	($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
	$SQL = <<SQLEND;
BEGIN
	$package.$procedure(:debug_in, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";

	$errcode = 0;
	$n_errors = 0;
	while (not $success and $n_errors < $MAX_ERRORS) {
		($DEBUG > 4) and write_log "\n";
		($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
		($DEBUG > 4) and write_log "CHECK> \n";
		($DEBUG > 4) and write_log "CHECK>          debug_in: $DEBUGFLAG\n";
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
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
		}
		else {
			$dbh->disconnect if defined($dbh);
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> Exceuting of procedure has failed\n";
			write_log "ERROR> Error_message:\n$errmsg\n";
			write_log "ERROR> Error_code:$errcode\n";
			write_log "ERROR> Error_line:\n$errline\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			$n_errors++;
			sleep $SLEEP_AFTER_ERROR;
			connect_Oracle;
		}

	}
	if ($errcode != 0) {
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                        W A R N I N G                              !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> Error_message:\n$errmsg\n";
			write_log "ERROR> Error_code:$errcode\n";
			write_log "ERROR> Error_line:\n$errline\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                        W A R N I N G                              !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: run_SQL_procedure - END\n";
	return $exit_cd;
}

################################################################################
# Funkce get_Data - get data
################################################################################
sub get_Data {
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: get_Data - START\n";
	$TABS = $TABS . "    ";
	$record = ();

	$SQL = <<SQLEND;
SELECT
	 log_event_id
	,error_cd
	,engine_name
	,NVL(system_name, 'No system')
	,job_name
	,job_id
	,severity
	,notification_type_cd	
	,event_ds
	,NVL(recommendation_ds, 'No recommendation')
	,NVL(note, 'No note')
	,NVL(address,'No address')
	,detected_ts
FROM STAT_LOG_MESSAGE_HIST
WHERE sent_ts IS NULL
ORDER BY detected_ts ASC
SQLEND
	$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
	$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
	$sth->bind_columns(undef, \$log_event_id, \$error_cd, \$engine_name, \$system_name, \$job_name, \$job_id, \$severity, \$notification_type_cd, \$event_ds, \$recommendation_ds, \$note, \$address, \$detected_ts);
	while( $sth->fetch() ) {
		$record->{$log_event_id}->{'error_cd'} = $error_cd;
		$record->{$log_event_id}->{'engine_name'} = $engine_name;
		$record->{$log_event_id}->{'system_name'} = $system_name;
		$record->{$log_event_id}->{'job_name'} = $job_name;
		$record->{$log_event_id}->{'job_id'} = $job_id;
		$record->{$log_event_id}->{'severity'} = $severity;
		$record->{$log_event_id}->{'notification_type_cd'} = $notification_type_cd;
		$record->{$log_event_id}->{'event_ds'} = $event_ds;
		$record->{$log_event_id}->{'recommendation_ds'} = $recommendation_ds;
		$record->{$log_event_id}->{'note'} = $note;
		$record->{$log_event_id}->{'address'} = $address;
		$record->{$log_event_id}->{'detected_ts'} = $detected_ts;
		$record->{$log_event_id}->{'sent_result'} = "";
		($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------------\n";
		($DEBUG > 4) and write_log "CHECK>         log_event_id: $log_event_id\n";
		($DEBUG > 4) and write_log "CHECK>             error_cd: $error_cd\n";
		($DEBUG > 4) and write_log "CHECK>             engine_name: $engine_name\n";
		($DEBUG > 4) and write_log "CHECK>             system_name: $system_name\n";
		($DEBUG > 4) and write_log "CHECK>             job_name: $job_name\n";
		($DEBUG > 4) and write_log "CHECK>               job_id: $job_id\n";
		($DEBUG > 4) and write_log "CHECK>             severity: $severity\n";
		($DEBUG > 4) and write_log "CHECK> notification_type_cd: $notification_type_cd\n";
		($DEBUG > 4) and write_log "CHECK>             event_ds: $event_ds\n";
		($DEBUG > 4) and write_log "CHECK>    recommendation_ds: $recommendation_ds\n";
		($DEBUG > 4) and write_log "CHECK>                 note: $note\n";
		($DEBUG > 4) and write_log "CHECK>              address: $address\n";
		($DEBUG > 4) and write_log "CHECK>          detected_ts: $detected_ts\n";
	}
	$sth->finish();   

	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: get_Data - END\n";
}

################################################################################
# Funkce sent_SNMP - send trap
################################################################################
sub sent_SNMP {
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: sent_SNMP - START\n";
	$TABS = $TABS . "    ";

	for $log_event_id (sort keys %$record) {
		$Log_Event_ID=$log_event_id;
		$Error_CD=$record->{$log_event_id}->{'error_cd'};
		$Engine_Name=$record->{$log_event_id}->{'engine_name'};
		$System_Name=$record->{$log_event_id}->{'system_name'};
		$Job_Name=$record->{$log_event_id}->{'job_name'};
		$Job_ID=$record->{$log_event_id}->{'job_id'};
		$Severity=$record->{$log_event_id}->{'severity'};
		$Notification_Type_CD=$record->{$log_event_id}->{'notification_type_cd'};
		$Event_Ds=$record->{$log_event_id}->{'event_ds'};
		$Recommendation_Ds=$record->{$log_event_id}->{'recommendation_ds'};
		$Note=$record->{$log_event_id}->{'note'};
		#$Address=$record->{$log_event_id}->{'address'};
		$Detected_TS=$record->{$log_event_id}->{'detected_ts'};

		$Param=$PARAM;
		$Param=$Param.$OID_Parse_Suffix.".1 s \"" . $Log_Event_ID . "\" ";
		$Param=$Param.$OID_Parse_Suffix.".2 s \"" . $Error_CD."\" ";
		$Param=$Param.$OID_Parse_Suffix.".3 s \"" . $Job_Name."\" ";
		$Param=$Param.$OID_Parse_Suffix.".4 s \"" . $Job_ID."\" ";
		$Param=$Param.$OID_Parse_Suffix.".5 s \"" . $Severity."\" ";
		$Param=$Param.$OID_Parse_Suffix.".6 s \"" . $Notification_Type_CD."\" ";
		$Param=$Param.$OID_Parse_Suffix.".7 s \"" . $Event_Ds."\" ";
		$Param=$Param.$OID_Parse_Suffix.".8 s \"" . $Recommendation_Ds."\" ";
		$Param=$Param.$OID_Parse_Suffix.".9 s \"" . $Note."\" ";
		$Param=$Param.$OID_Parse_Suffix.".10 s \"" . $Detected_TS . "\" ";
		$Param=$Param.$OID_Parse_Suffix.".11 s \"" . $Engine_Name . "\" ";
		$Param=$Param.$OID_Parse_Suffix.".12 s \"" . $System_Name . "\" ";

		# na zaver posleme prislusny SNMP trap
		$cmd = "$SNMPTRAP " . $Param;
		($DEBUG > 4) and write_log "CHECK> CMD: $cmd\n";
 	
		$result = `$cmd`;
 
		if (length($result) ne 0) {
			write_log "ERROR> Sending SNMP $Log_Event_ID for job $Job_Name has failed\n";
			write_log "ERROR> $result\n";
			write_log "ERROR> $cmd\n";
			$cmd =~ s/\"//g; # delete "
			$cmd_err = "logger -p local5.alert -t SNIGGER  \"".$cmd."\"";
			$result = `$cmd_err`;
			$record->{$log_event_id}->{'sent_result'} = "ERROR";
		}
		else {
			$record->{$log_event_id}->{'sent_result'} = "OK";
			($DEBUG > 4) and write_log "CHECK> \n";
			($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
			($DEBUG > 4) and write_log "CHECK> SNMP $Log_Event_ID for job $Job_Name sent successfully\n";
			($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
			($DEBUG > 4) and write_log "CHECK> \n";
		}
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: sent_SNMP - END\n";
	return $exit_cd;
}

################################################################################
# Funkce write_Result - write result into table
################################################################################
sub write_Result {
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: write_Result - START\n";
	$TABS = $TABS . "    ";
	$n_messages = 0;
	$n_messages_ok = 0;

	for $log_event_id (sort keys %$record) {
		$n_messages++;
		if ($record->{$log_event_id}->{'sent_result'} eq "OK") {
			$n_messages_ok++;
			$SQL = <<SQLEND;
UPDATE STAT_LOG_MESSAGE_HIST
SET sent_ts = CURRENT_TIMESTAMP
WHERE log_event_id = $log_event_id
SQLEND

			($DEBUG > 3) and write_log "  SQL>\n$SQL\n";
	
			$success = 0;
			while (not $success) {
				$sth = $dbh->prepare($SQL) or report_warning ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
				$sth->execute() or report_warning ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
				if ($exit_cd == 0) {
					$success = 1;
					($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
					($DEBUG > 4) and write_log "CHECK>           exit_cd: $exit_cd\n";
					($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
					($DEBUG > 4) and write_log "CHECK> Statement executed successfully\n";
				}
				else {
					$dbh->disconnect if defined($dbh);
					write_log "\n";
					write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
					write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
					write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
					write_log "ERROR> Exceuting of SQL has failed\n";
					write_log "ERROR> Error_message:\n$errmsg\n";
					write_log "ERROR> Error_code:$errcode\n";
					write_log "ERROR> Error_line:\n$errline\n";
					write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
					write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
					write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
					write_log "\n";
					sleep $DELAY_AFTER_ERROR;
					connect_Oracle;
				}
			}
		}
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: write_Result - END\n";
	return $exit_cd;
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

$| = 1; 					# cache switch off
$TABS = "";




write_log "\n";
write_log "################################################################################\n";
write_log "################################################################################\n";
write_log "###                                                                          ###\n";
write_log "###                                 S T A R T                                ###\n";
write_log "###                                                                          ###\n";
write_log "################################################################################\n";
write_log "################################################################################\n";



($DEBUG > 2) and write_log "\n";
($DEBUG > 2) and write_log "DEBUG> =========================================================================\n";
($DEBUG > 2) and write_log "DEBUG> ===               P a r a m e t t e r s   s e t t i n g s            ====\n";
($DEBUG > 2) and write_log "DEBUG>             DEBUG: $DEBUG\n";
($DEBUG > 2) and write_log "DEBUG>      Oracle debug: $DEBUGFLAG\n";
($DEBUG > 2) and write_log "DEBUG>            -debug: $debugflag\n";
($DEBUG > 4) and write_log "CHECK>            SYSTEM: $SYSTEM\n";
($DEBUG > 2) and write_log "DEBUG> =========================================================================\n";

connect_Oracle;
$exit_cd = 0;
($exit_cd == 0) and $exit_cd = run_SQL_procedure ("PCKG_FWRK","SP_FWRK_CHECK_WD_STATUS");
($exit_cd == 0) and $exit_cd = run_SQL_procedure ("PCKG_FWRK","SP_FWRK_CHECK_SCHED_STATUS");
($exit_cd == 0) and $exit_cd = run_SQL_procedure ("PCKG_FWRK","SP_FWRK_CHECK_INITIALIZATION");
($exit_cd == 0) and $exit_cd = run_SQL_procedure ("PCKG_FWRK","SP_FWRK_CHECK_NOTIFICATION");
($exit_cd == 0) and $exit_cd = run_SQL_procedure ("PCKG_FWRK","SP_FWRK_MESSAGE_GEN");

get_Data;
sent_SNMP;
write_Result;

write_log "\n";
write_log "********************************************************************************\n";
write_log "***                             F I N I S H                                  ***\n";
write_log "********************************************************************************\n";
write_log "\n";
write_log "Finished with exit_cd = $exit_cd\n";
write_log "\n";
write_log "Programm successfully sent $n_messages_ok of $n_messages messages\n";
write_log "\n";
write_log "********************************************************************************\n";
write_log "***                             F I N I S H                                  ***\n";
write_log "********************************************************************************\n";
write_log "\n";
$dbh->disconnect if defined($dbh);
exit $exit_cd;
