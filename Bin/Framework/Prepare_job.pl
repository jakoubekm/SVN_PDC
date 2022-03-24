#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  Name:		Prepare_job.pl
#  IN_parameters:	-file $BAT -id $job_id -name $job_name -type $job_type -queue $queue_number -engine $ENGINE_ID -system $SYSTEM [-x $DEBUG] [-debug]
#  OUT_paramaters:	exit_cd
#  Called from:		Engine.pl
#  Calling:		Run_job.pl
#-------------------------------------------------------------------------------
#  Project:		PDC
#  Author:		Teradata - Petr Stefanek
#  Date:		2010-02-10
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
#-------------------------------------------------------------------------------


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
use Getopt::Long;
use File::Spec;

$| = 1;	# vypnuti cache

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

$LOGFILE = File::Spec->catfile("$ENV{'PMWorkflowLogDir'}", "BinLogs", "__Prepare_job_@" . $SYSTEM . "@" . $ENGINE_ID . "_" . $queue_number . ".log");

$WORKDIR = File::Spec->catfile("$ENV{'PMRootDir'}", "Bin", "Framework");        # directory where PERL scripts are stored
$PERLEXE = "$ENV{'PERLEXE'}";                   # PERL executable

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
($DEBUG > 4) and write_log "CHECK>        DEBUG: $DEBUG\n";
($DEBUG > 4) and write_log "CHECK> Oracle debug: $DEBUGFLAG\n";
($DEBUG > 4) and write_log "CHECK> ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";

$programm = $ENV{"PERLEXE"};
$arguments = $PERLEXE . " " . File::Spec->catfile($WORKDIR, "Run_job.pl -file $BAT -id $job_id -name $job_name -type $job_type -queue $queue_number -engine $ENGINE_ID -system $SYSTEM -x $DEBUG $debugflag");
($DEBUG > 5) and write_log "TRACE> \n";
if (uc($^O) eq "MSWIN32") {
	($DEBUG > 5) and write_log "TRACE> Starting programm: $programm\n";
	($DEBUG > 5) and write_log "TRACE> Programm arguments: $arguments\n";
	Win32::Process::Create($ProcessObj,
	"$programm",
	"$arguments",
	0,
	NORMAL_PRIORITY_CLASS,
	".") or report_error "ERROR> !!! Process start failed\n";
}
elsif ((uc($^O) eq "SOLARIS") or (uc($^O) eq "LINUX")) {
	($DEBUG > 5) and write_log "TRACE> Programm and arguments: $arguments\n";
	my $thr = threads->create(sub {system($arguments);});
	select(undef, undef, undef, 0.1);		# wait 100 ms if thread realy started
	if (!$thr->is_running()){
		report_error "ERROR> !!! Process start failed\n";
	}
}
($DEBUG > 5) and write_log "TRACE> -------------------------------------------------------------------------\n";
($DEBUG > 5) and write_log "TRACE> \n";
exit 0;
