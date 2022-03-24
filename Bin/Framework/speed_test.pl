#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  Name:		Init_Prepare.pl
#  IN_parameters:	-engine $ENGINE_ID [-x $DEBUG] [-debug]
#  OUT_paramaters:	exit_cd
#  Called from:		N/A
#  Calling:		NONE
#-------------------------------------------------------------------------------
#  Project:		PDC
#  Author:		Teradata - Petr Stefanek
#  Date:		2011-09-22
#-------------------------------------------------------------------------------
#  Version:		1.0
#-------------------------------------------------------------------------------
#  Description:		Script prepares PDC initialization
#-------------------------------------------------------------------------------
#  Version:
#  Modified:
#  Date:
#  Modification:
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

use Getopt::Long;
use DBI;
# We need DBD::Oracle data types for REF Cursor variable
use DBD::Oracle qw(:ora_types);
use XML::Simple;
use File::Spec;

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

	$dbh = DBI->connect( $PDCserver, $PDCuser, $PDCpassword ) || report_error( "ERROR> Couldn't connect to database:\n" . $DBI::errstr . "\n" );
	$dbh->{AutoCommit}    = 0;
	$dbh->{RaiseError}    = 1;
	$dbh->{ora_check_sql} = 0;
	$dbh->{RowCacheSize}  = 16;

	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: connect_Oracle - END\n";
	return $step;
}

################################################################################
# Funkce SP_INIT_PREPARE - inicializace
################################################################################
sub SP_INIT_PREPARE {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: SP_INIT_PREPARE - START\n";
	$TABS = $TABS . "    ";
	$job_name = 'Cleaning';

	$SQL = <<SQLEND;
BEGIN
	PCKG_ENGINE.SP_ENG_GET_LOAD_DATE(:job_name_in, :debug_in, :job_id_out, :load_date_out, :pid_out, :exit_cd, :errmsg_out, :errcode_out, :errline_out);
END;
SQLEND

	($DEBUG > 3) and write_log "  SQL>\n$SQL\n";
	my $success = 0;
	while (not $success) {
		($DEBUG > 4) and write_log "\n";
		($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
		($DEBUG > 4) and write_log "CHECK>          debug_in: $DEBUGFLAG\n";
		$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
		$sth->bind_param_inout( ":job_name_in", \$job_name, 20);
		$sth->bind_param_inout( ":debug_in", \$DEBUGFLAG, 20);
		$sth->bind_param_inout( ":job_id_out", \$job_id_out, 20);
		$sth->bind_param_inout( ":load_date_out", \$load_date_out, 20);
		$sth->bind_param_inout( ":pid_out", \$pid_out, 20);
		$sth->bind_param_inout( ":exit_cd", \$exit_cd, 20);
		$sth->bind_param_inout( ":errmsg_out", \$errmsg, 10000);
		$sth->bind_param_inout( ":errcode_out", \$errcode, 20);
		$sth->bind_param_inout( ":errline_out", \$errline, 10000);
		$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
		if ($exit_cd == 0) {
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
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: SP_INIT_PREPARE - END\n";
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

$SYSTEM = $ENV{'SYSTEMNAME'};                   # System name used for selecting values from system_info.xml
$CONFIG_FILE = File::Spec->catfile("$ENV{'PMRootDir'}", "Security", "Passwords", "system_info.xml");
$LOGFILE = File::Spec->catfile("$ENV{'PMWorkflowLogDir'}", "BinLogs", "____Init_Prepare@" . $ENGINE_ID . ".log");

write_log "\n";
write_log "################################################################################\n";
write_log "################################################################################\n";
write_log "###                                                                          ###\n";
write_log "###                                 S T A R T                                ###\n";
write_log "###                                                                          ###\n";
write_log "################################################################################\n";
write_log "################################################################################\n";
write_log "\n";

($DEBUG > 4) and write_log "\n";
($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
($DEBUG > 4) and write_log "CHECK> ~~~                J o b   p a r a m e t e r s                        ~~~\n";
($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
($DEBUG > 4) and write_log "CHECK>    ENGINE_ID: $ENGINE_ID\n";
($DEBUG > 4) and write_log "CHECK> -------------------------------------------------------------------------\n";
($DEBUG > 4) and write_log "CHECK>       SYSTEM: $SYSTEM\n";
($DEBUG > 4) and write_log "CHECK>        DEBUG: $DEBUG\n";
($DEBUG > 4) and write_log "CHECK> Oracle debug: $DEBUGFLAG\n";
($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
($DEBUG > 4) and write_log "\n";

write_log "START\n";
connect_Oracle;

SP_INIT_PREPARE;

$dbh->disconnect if defined($dbh);
write_log "   job_id: $job_id_out\n";
write_log "load_date: $load_date_out\n";
write_log "      pid: $pid_out\n";
write_log "FINISH\n";
exit 0;
