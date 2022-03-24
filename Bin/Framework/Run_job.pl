#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  Name:		Run_job.pl
#  IN_parameters:	-file $BAT -id $job_id -name $job_name -type $job_type -queue $queue_number -engine $ENGINE_ID -system $SYSTEM [-x $DEBUG] [-debug]
#  OUT_paramaters:	exit_cd
#  Called from:		Prepare_job.pl
#  Calling:		NONE
#-------------------------------------------------------------------------------
#  Project:		PDC
#  Author:		Teradata - Petr Stefanek
#  Date:		2010-02-10
#-------------------------------------------------------------------------------
#  Version:		2.0
#-------------------------------------------------------------------------------
#  Description:		Script runs the job
#-------------------------------------------------------------------------------
#  Version:		2.0
#  Modified:		PSt
#  Date:		2015-02-24
#  Modification:	Double Engine support
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

use Getopt::Long;
use DBI;
# We need DBD::Oracle data types for REF Cursor variable
use DBD::Oracle qw(:ora_types);
use XML::Simple;
use Sys::Hostname;
use File::Spec;

$EXT_EXIT_CD_TYPE = "DATA_QUALITY";		# job_type jobu pro kontrolu datove kvality (ma rozsireny exit_cd)
$DELAY_AFTER_ERROR = 5;

################################################################################
################################################################################
#                                                                              #
#                                   S U B S                                    #
#                                                                              #
################################################################################
################################################################################


################################################################################
# SUB write_log - zapis zpravy do LOGFILE
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
# SUB write_f_log - zapis strukturovane zpravy do LOGFILE
################################################################################
sub write_f_log {
	@time = localtime(time);
	$year = 1900 + $time[5];
	$month = 1 + $time[4];
	if(open LOG, ">>$LOGFILE"){
		printf LOG "%4d-%02d-%02d %02d:%02d:%02d  ", $year, $month, $time[3], $time[2], $time[1], $time[0];
		printf LOG @_;
	       	close LOG;
	}
	printf "%4d-%02d-%02d %02d:%02d:%02d  ", $year, $month, $time[3], $time[2], $time[1], $time[0];
	printf @_;
	return 0;
}

################################################################################
# SUB report_error - zapis error zpravy do LOGFILE a exit
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
# SUB report_warning - zapis error zpravy do LOGFILE
################################################################################
sub report_warning {
	write_log "\n";
	write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
	write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
	write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
	write_log "\n";
	write_log @_;
	write_log "\n";
	write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
	write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
	write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
}

################################################################################
# Funkce connect_Oracle -connect do Oracle database
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
	$dbh->{RaiseError}    = 0;
	$dbh->{ora_check_sql} = 0;
	$dbh->{RowCacheSize}  = 16;

	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: connect_Oracle - END\n";
	return $step;
}

################################################################################
# Funkce confirm_Loading_State - potvrdi load tabulky v SESS_SRCTABLE
################################################################################
sub confirm_Loading_State($) {
  my ($loading_state) = @_;
  
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: confirm_Loading_State '$loading_state'- START\n";
	$TABS = $TABS . "    ";
	$success = 0;
	$SQL = <<SQLEND;
	UPDATE SESS_SRCTABLE
	SET load_status = '$loading_state'
	WHERE job_id = NVL(
	  NVL(
		(
			SELECT SJ2.job_id
			FROM SESS_JOB SJ1
			JOIN SESS_JOB SJ2
			ON SJ1.stream_id = SJ2.stream_id
			WHERE SJ2.job_type = 'VALIDATOR'
			AND SJ1.job_id = $job_id
		)
		,
		(
			SELECT SJ2.job_id
			FROM SESS_JOB SJ1
			JOIN SESS_JOB SJ2
			ON SJ1.stream_id = SJ2.stream_id
			WHERE SJ2.job_type = 'CHECKER'
			AND SJ1.job_id = $job_id
		)
		)
	, -1)
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";

	while (not $success) {
		$exit_cd = 0;
		$sth = $dbh->prepare($SQL) or (report_warning ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n") and $exit_cd = 1);
		$sth->execute() or (report_warning ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n") and $exit_cd = 1);
		if ($exit_cd == 0) {
			$success = 1;
			($DEBUG > 4) and write_log "CHECK>           exit_cd: $exit_cd\n";
			($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
			($DEBUG > 4) and write_log "CHECK> Statement executed successfully\n";
			$sth->finish();
    			$dbh->commit();
		}
		else {
			$dbh->disconnect if defined($dbh);
			write_log "\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> Exceuting of procedure has failed\n";
			(defined $errmsg) and write_log "ERROR> Error_message:\n$errmsg\n";
			(defined $errcode) and write_log "ERROR> Error_code:$errcode\n";
			(defined $errline) and write_log "ERROR> Error_line:\n$errline\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "ERROR> !!!                        E  R  R  O  R                              !!!\n";
			write_log "ERROR> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
			write_log "\n";
			sleep $DELAY_AFTER_ERROR;
			connect_Oracle;
		}
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: confirm_Loading_State - END\n";
	return $exit_cd;
}

################################################################################
# Funkce SP_ENG_UPDATE_STATUS - update statusu ulohy
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
		$sth->bind_param_inout( ":return_status_out", \$step, 0);
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
				sleep $DELAY_AFTER_ERROR;
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
# Funkce SP_ENG_UPDATE_RUNNIG_JOB_PID - update PID in sess queue
################################################################################
sub SP_ENG_UPDATE_RUNNIG_JOB_PID {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: SP_ENG_UPDATE_STATUS - START\n";
	$TABS = $TABS . "    ";

	my $running_job_pid_in= $$;
	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_UPDATE_RUNNIG_JOB_PID(:job_id_in, :engine_id_in, :queue_number_in, :running_job_pid_in, :debug_in, :return_status_out, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
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
		($DEBUG > 4) and write_log "CHECK>      engine_id_in: $ENGINE_ID\n";
		($DEBUG > 4) and write_log "CHECK>   queue_number_in: $queue_number\n";
		($DEBUG > 4) and write_log "CHECK>   running_job_pid_in: $running_job_pid_in\n";
		($DEBUG > 4) and write_log "CHECK>          debug_in: $DEBUGFLAG\n";
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
		$sth->bind_param_inout( ":job_id_in", \$job_id, 20);
		$sth->bind_param_inout( ":engine_id_in", \$ENGINE_ID, 20);
		$sth->bind_param_inout( ":queue_number_in", \$queue_number, 20);
		$sth->bind_param_inout( ":running_job_pid_in", \$running_job_pid_in, 20);
		$sth->bind_param_inout( ":return_status_out", \$step, 0);
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
			$success = 1;
			
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
$TABS = "";

GetOptions(
	"engine=i"=> \$ENGINE_ID,
	"system=s"=> \$SYSTEM,
	"file=s"=> \$BAT,
	"id=i"=> \$job_id,
	"name=s"=> \$job_name,
	"type=s"=> \$job_type,
	"queue=i"=> \$queue_number,
	"x=i"=> \$DEBUG,
	"debug"=> \$DEBUGFLAG
	);
if (not defined $ENGINE_ID) { $ENGINE_ID = 0; }
if (not defined $DEBUG) { $DEBUG = 0; }
if (not defined $DEBUGFLAG) {
	$DEBUGFLAG = 0;
	$debugflag = "";
}
else {
	$debugflag = "-debug";
}
if (uc($^O) eq "MSWIN32") {
        $BAT = $BAT;
}
elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
        $BAT = "sh " . $BAT;
}
else {
        print "ERROR> Unsupported operating system, exiting...\n";
        exit 9;
}


$SYSTEM = $ENV{'SYSTEMNAME'};                   # System name used for selecting values from system_info.xml
$CONFIG_FILE = File::Spec->catfile("$ENV{'PMRootDir'}", "Security", "Passwords", "system_info.xml");
$LOGFILE = File::Spec->catfile("$ENV{'PMWorkflowLogDir'}", "BinLogs", "_Run_job@" .$SYSTEM . "@" . $ENGINE_ID . "_" . $queue_number .  ".log");

($DEBUG > 2) and write_log "DEBUG> CALL SP_ENG_UPDATE_RUNNIG_JOB_PID ($request, 0)\n";
connect_Oracle;
SP_ENG_UPDATE_RUNNIG_JOB_PID;
$dbh->disconnect if defined($dbh);


($DEBUG > 2) and write_log "DEBUG>\n";
($DEBUG > 2) and write_log "DEBUG>\n";
($DEBUG > 2) and write_log "DEBUG>\n";
($DEBUG > 2) and write_log "DEBUG> #########################################################################\n";
write_log "*****  Starting job: $job_name\n";
($DEBUG > 2) and write_log "DEBUG> #########################################################################\n";
($DEBUG > 2) and write_log "DEBUG>\n";
($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
($DEBUG > 4) and write_log "CHECK> ~~~                J o b   p a r a m e t e r s                        ~~~\n";
($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
($DEBUG > 4) and write_log "CHECK>    ENGINE_ID: $ENGINE_ID\n";
($DEBUG > 4) and write_log "CHECK>       SYSTEM: $SYSTEM\n";
($DEBUG > 4) and write_log "CHECK>       job_id: $job_id\n";
($DEBUG > 4) and write_log "CHECK>     job_name: $job_name\n";
($DEBUG > 4) and write_log "CHECK>     job_type: $job_type\n";
($DEBUG > 4) and write_log "CHECK>     BAT file: $BAT\n";
($DEBUG > 4) and write_log "CHECK> queue_number: $queue_number\n";
($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
($DEBUG > 4) and write_log "CHECK>       SYSTEM: $SYSTEM\n";
($DEBUG > 4) and write_log "CHECK>        DEBUG: $DEBUG\n";
($DEBUG > 4) and write_log "CHECK> Oracle debug: $DEBUGFLAG\n";
($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
($DEBUG > 4) and write_log "\n";
($DEBUG > 2) and write_log "DEBUG> %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n";
if (uc($^O) eq "MSWIN32") {
	$BATLOGFILE = File::Spec->catfile("$ENV{'PMWorkflowLogDir'}", "BinLogs", "_Run_job@".$SYSTEM . "@" . $ENGINE_ID . "_" . $queue_number ."_BAT_EXEC" .  ".log");
	$exitcode = system ("$BAT >$BATLOGFILE 2>&1");
}
elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
	$exitcode = system ("$BAT >/dev/null 2>&1");
}
($DEBUG > 2) and write_log "DEBUG> Exitcode for job $job_name: $exitcode\n";
($DEBUG > 2) and write_log "DEBUG> %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n";

if ($exitcode == 0 and ($job_type eq "CHECKER" )) {
	connect_Oracle;
	confirm_Loading_State("CHECKED");
	$dbh->disconnect if defined($dbh);
}

if ($exitcode == 0 and ($job_type eq "LOADER_STG" )) {
	connect_Oracle;
	confirm_Loading_State("LOADED");
	$dbh->disconnect if defined($dbh);
}

if ($exitcode == 0 and ($job_type eq "UNIFICATION" )) {
	connect_Oracle;
	confirm_Loading_State("UNIFIED");
	$dbh->disconnect if defined($dbh);
}
	
if ($exitcode == 0) {
	$request = "SUCCESS";
}
#elsif (uc($job_type) eq "DATASTAGE" and ($exitcode == 0 or $exitcode == 256 or $exitcode == 512)) {
#	$request = "SUCCESS";
#}
elsif ($job_type eq $EXT_EXIT_CD_TYPE and $exitcode = -1024) {
	$request = "DQ_INFO";
}
elsif ($job_type eq $EXT_EXIT_CD_TYPE and $exitcode = -2048) {
	$request = "DQ_WAARNING";
}
elsif ($job_type eq $EXT_EXIT_CD_TYPE and $exitcode = -3072) {
	$request = "DQ_ERROR";
}
elsif ($job_type eq $EXT_EXIT_CD_TYPE and $exitcode = -4096) {
	$request = "DQ_CRITICAL";
}
elsif ($job_type eq $EXT_EXIT_CD_TYPE and $exitcode = -5120) {
	$request = "DQ_CRITICAL_OTHR";
}
elsif ($job_type eq 'RUN_SCRIPT_INTERVAL' and $exitcode == 2304) {
	$request = "WARN_NOTFINISHED";
}
else {
	$request = "FAILED";
}
($DEBUG > 4) and write_log "CHECK>      request: $request\n";
($DEBUG > 2) and write_log "DEBUG> CALL SP_ENG_UPDATE_STATUS ($request, 0)\n";
connect_Oracle;
SP_ENG_UPDATE_STATUS ($request, 0);
$dbh->disconnect if defined($dbh);
exit $exitcode;
