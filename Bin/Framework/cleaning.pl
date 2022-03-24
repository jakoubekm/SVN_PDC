# Cleaning.pl

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  Name:         Cleaning.pl
#  Parameters:   ENGINE_ID, DEBUG, DEBUGFLAG
#
#  Project:      PDC
#  Author:       Teradata - Petr Stefanek, Vladimir Duchon
#  Date:         2011-11-01
#-------------------------------------------------------------------------------
#  Description:  Script moves old logs ...
#-------------------------------------------------------------------------------
#  Modified:     Vladimir Duchon   
#  Date:         2012-09-03   
#  Modification: Reading configuration form system_info.xml file. Archive split.   
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

#!E:/Perl/bin/perl -w


use Getopt::Long;
use DBI;
# We need DBD::Oracle data types for REF Cursor variable
use DBD::Oracle qw(:ora_types);
use XML::Simple;
use Sys::Hostname;
use File::stat;
use File::Copy;
use File::Path;

$AFTERFLAG = 0;

GetOptions(
	"engine=i"=> \$ENGINE_ID,
	"x=i"=> \$DEBUG,
	"after"=> \$AFTERFLAG,
	"debug"=> \$DEBUGFLAG
	);
if (not defined $ENGINE_ID) { $ENGINE_ID = 0; }
if (not defined $DEBUG) { $DEBUG = 0; }
if ($DEBUG > 8) {
	#$DEBUG = 8; 			# don't comment this line from security reason
}
if (not defined $DEBUGFLAG) {
	$DEBUGFLAG = 0;
	$debugflag = "";
}
else {
	$debugflag = "-debug";
}
($DEBUG > 4) and print "CHECK> ENGINE_ID: $ENGINE_ID\n";
($DEBUG > 4) and print "CHECK> AFTERFLAG: $AFTERFLAG\n";
($DEBUG > 4) and print "CHECK>     DEBUG: $DEBUG\n";

print "Unprocessed by Getopt::Long\n" if $ARGV[0];
foreach (@ARGV) {
	print "$_\n";
}

$| = 1; # vypnuti cache


################################################################################
# Konfigurovatelne parametry
#

$SYSTEM = $ENV{"SYSTEMNAME"};			# jmeno PDC systemu
$CONFIG_FILE = File::Spec->catfile($ENV{"PMRootDir"},"Security","Passwords","system_info.xml");
$LOGFILE = File::Spec->catfile($ENV{"PMWorkflowLogDir"},"BinLogs","__Cleaning_@" . $ENGINE_ID . ".log");
$PMROOT_DIRECTORY = $ENV{"PMRootDir"};	# root adresar pro PM
$LOG_DIRECTORY = $ENV{"PMWorkflowLogDir"};		# root adresar pro logy

# ostatni konfigurace - viz default nize - se nacita pres sub get_configuration
$REPOSITORY_ARCHIVE_DIR="";
$EXTRACTS_ARCHIVE_DIR="";
$DATA_ARCHIVE_DIR="";
$LOGS_ARCHIVE_DIR="";

$EXTRACTS_ARCHIVE_EXPIRATION_DAYS=-1;
$DATA_ARCHIVE_EXPIRATION_DAYS=-1;
$LOGS_ARCHIVE_EXPIRATION_DAYS=-1;


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
# Funkce get_configuration - get the information about directories etc. from system_info.xml
################################################################################
sub get_configuration {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: get_configuration - START\n";
	$TABS = $TABS . "    ";

	$config = XMLin($CONFIG_FILE);
	$REPOSITORY_ARCHIVE_DIR=$config->{connection}->{$SYSTEM}->{REPOSITORY_ARCHIVE_DIR};
	$EXTRACTS_ARCHIVE_DIR=$config->{connection}->{$SYSTEM}->{EXTRACTS_ARCHIVE_DIR};
	$DATA_ARCHIVE_DIR=$config->{connection}->{$SYSTEM}->{DATA_ARCHIVE_DIR};
	$LOGS_ARCHIVE_DIR=$config->{connection}->{$SYSTEM}->{LOGS_ARCHIVE_DIR};
	
	$EXTRACTS_ARCHIVE_EXPIRATION_DAYS=$config->{connection}->{$SYSTEM}->{EXTRACTS_ARCHIVE_EXPIRATION_DAYS};
	$DATA_ARCHIVE_EXPIRATION_DAYS=$config->{connection}->{$SYSTEM}->{DATA_ARCHIVE_EXPIRATION_DAYS};
	$LOGS_ARCHIVE_EXPIRATION_DAYS=$config->{connection}->{$SYSTEM}->{LOGS_ARCHIVE_EXPIRATION_DAYS};
	
	
	($DEBUG > 4) and write_log "CHECK>   REPOSITORY_ARCHIVE_DIR: $REPOSITORY_ARCHIVE_DIR\n";
	($DEBUG > 4) and write_log "CHECK>   EXTRACTS_ARCHIVE_DIR: $EXTRACTS_ARCHIVE_DIR\n";
	($DEBUG > 4) and write_log "CHECK>   DATA_ARCHIVE_DIR: $DATA_ARCHIVE_DIR\n";
	($DEBUG > 4) and write_log "CHECK>   LOGS_ARCHIVE_DIR: $LOGS_ARCHIVE_DIR\n";
	($DEBUG > 4) and write_log "CHECK>   EXTRACTS_ARCHIVE_EXPIRATION_DAYS: $EXTRACTS_ARCHIVE_EXPIRATION_DAYS\n";
	($DEBUG > 4) and write_log "CHECK>   DATA_ARCHIVE_EXPIRATION_DAYS: $DATA_ARCHIVE_EXPIRATION_DAYS\n";
	($DEBUG > 4) and write_log "CHECK>   LOGS_ARCHIVE_EXPIRATION_DAYS: $LOGS_ARCHIVE_EXPIRATION_DAYS\n";
	
	if(
	($REPOSITORY_ARCHIVE_DIR eq "")
	|| ($EXTRACTS_ARCHIVE_DIR eq "")
	|| ($DATA_ARCHIVE_DIR eq "")
	|| ($LOGS_ARCHIVE_DIR eq "")
	|| ($EXTRACTS_ARCHIVE_EXPIRATION_DAYS == -1)
	|| ($DATA_ARCHIVE_EXPIRATION_DAYS == -1)
	|| ($LOGS_ARCHIVE_EXPIRATION_DAYS == -1)
	)
	{
		report_error( "ERROR> Couldn't get configuration from system_info.xml:\n" );
	}
	

	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: get_configuration - END\n";
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
}

################################################################################
# Funkce get_Dttm - get datetime
################################################################################
sub get_Dttm {
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: get_Dttm - START\n";
	$TABS = $TABS . "    ";
	connect_Oracle;

	$SQL = <<SQLEND;
SELECT
	 TO_CHAR(param_val_date, 'YYYYMMDDHH24MISS')
FROM CTRL_PARAMETERS
WHERE param_name = 'PREV_LOAD_DATE'
AND param_cd = $ENGINE_ID
SQLEND
	$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
	$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
	$sth->bind_columns(undef, \$load_dttm);
	$sth->fetch;
	($DEBUG > 4) and write_log "CHECK>         load_dttm: $load_dttm\n";
# $SQL = <<SQLEND;
# SELECT
# 	 param_val_int
# FROM CTRL_PARAMETERS
# WHERE param_name = 'LOAD_SEQ_NUM'
# AND param_cd = $ENGINE_ID
# SQLEND
# 	$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
# 	$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
# 	$sth->bind_columns(undef, \$load_seq_num);
# 	$sth->fetch;
# 	($DEBUG > 4) and write_log "CHECK>         load_seq_num: $load_seq_num\n";
# 	$sth->finish();   
# 	$dbh->disconnect if defined($dbh);
# 	$load_dttm = $load_dttm . '_' . $load_seq_num;
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: get_Dttm - END\n";
}

################################################################################
# Funkce get_Dttm_after - get data
################################################################################
sub get_Dttm_after {
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: get_Dttm_after - START\n";
	$TABS = $TABS . "    ";
	connect_Oracle;

	$SQL = <<SQLEND;
SELECT
	 TO_CHAR(param_val_date, 'YYYYMMDDHH24MISS')
FROM CTRL_PARAMETERS
WHERE param_name = 'LOAD_DATE'
AND param_cd = $ENGINE_ID
SQLEND
	$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
	$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
	$sth->bind_columns(undef, \$load_dttm);
	$sth->fetch;
	($DEBUG > 4) and write_log "CHECK>         load_dttm: $load_dttm\n";
	$SQL = <<SQLEND;
SELECT
	 param_val_int
FROM CTRL_PARAMETERS
WHERE param_name = 'LOAD_SEQ_NUM'
AND param_cd = $ENGINE_ID
SQLEND
	$sth = $dbh->prepare($SQL) or report_error ("ERROR> Couldn't prepare statement:\n" . $dbh->errstr . "\n");
	$sth->execute() or report_error ("ERROR> Counldn't execute statement:\n" . $sth->errstr . "\n");;
	$sth->bind_columns(undef, \$load_seq_num);
	$sth->fetch;
	($DEBUG > 4) and write_log "CHECK>         load_seq_num: $load_seq_num\n";
	$sth->finish();   
	$dbh->disconnect if defined($dbh);
	$load_dttm = $load_dttm . '_' . $load_seq_num;
	$sth->finish();   
	$dbh->disconnect if defined($dbh);
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: get_Dttm_after - END\n";
}

################################################################################
# Funkce move_directory - presun vsech souboru do Old odresaru
################################################################################
sub move_directory {
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: move_directory - START\n";
	$TABS = $TABS . "    ";

	my $current_dir = shift;
	my $new_dir = shift;

	($DEBUG > 2) and write_log "DEBUG> Creating $new_dir directory\n";
	-d $new_dir or mkpath ($new_dir);
	chdir $current_dir;
	opendir( DIR, $current_dir);
	@allfiles = readdir DIR;
	closedir DIR;
	foreach (@allfiles) {
		($DEBUG > 5) and write_log "TRACE> Moving \t$_ \tfile into \t$new_dir directory\n";
    		-f $_ and move($_, File::Spec->catfile($new_dir ,  $_)); 
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: move_directory - END\n";
}

################################################################################
# Funkce copy_directory - kopie vsech souboru do Old odresaru
################################################################################
sub copy_directory {
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: copy_directory - START\n";
	$TABS = $TABS . "    ";

	my $current_dir = shift;
	my $new_dir = shift;

	($DEBUG > 2) and write_log "DEBUG> Creating $new_dir directory\n";
	-d $new_dir or mkpath ($new_dir);
	chdir $current_dir;
	opendir( DIR, $current_dir);
	@allfiles = readdir DIR;
	closedir DIR;
	foreach (@allfiles) {
		($DEBUG > 5) and write_log "TRACE> Copying \t$_ \tfile into \t$new_dir directory\n";
    		-f $_ and copy ($_, File::Spec->catfile($new_dir , $_));
	}
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: copy_directory - END\n";
}

################################################################################
# Funkce remove_files - odmazava soubory/adresare podle zadaneho poctu dni
################################################################################
sub remove_files {
	($DEBUG > 1) and write_log "\n";
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: remove_files - START\n";
	$TABS = $TABS . "    ";

	my $old_dir = shift;
	my $new_dir = shift;
	my $number_of_days = shift;
	my $del_dir = shift;      # Y = smaze i adresar, N = maze pouze soubory
	my $Cr_Mod = shift;       # C = maze podle data vytvoreni, M = podle data zmìny
	
	if(
		(length($old_dir)==0)
		|| (length($new_dir)==0)
		|| (length($number_of_days)==0)
		|| (length($del_dir)==0)
		|| (length($Cr_Mod)==0)
		|| ($number_of_days<0)		
	) {
			report_error( "ERROR> Inapropriate parameters for files deletion.\n" );
	}

	($DEBUG > 2) and write_log "DEBUG> Inspecting old files from $new_dir\n";
	$now = time();
	chdir $new_dir;
	opendir(DIR, $new_dir);
	my @alldirs = readdir DIR;
	closedir DIR;
	foreach (@alldirs) {
		next if (substr($_,0,1) eq ".");
		if(-d $_) {
			remove_files($new_dir, File::Spec->catfile($new_dir , $_), $number_of_days, $del_dir, $Cr_Mod);
		}
		if(-f $_) {
			$st = stat($_);
			if ($Cr_Mod eq "M") {
				if( ($now - $st->mtime)/3600/24 > $number_of_days and -f $_) {
					($DEBUG > 4) and write_log "CHECK>\t!!! Removing file $new_dir/$_\n";
					unlink $_;
				}
			}
			if ($Cr_Mod eq "C") {
				if( ($now - $st->ctime)/3600/24 > $number_of_days and -f $_) {
					($DEBUG > 4) and write_log "CHECK>\t!!! Removing file $new_dir/$_\n";
					unlink $_;
				}
			}
		}
		elsif(-d $_ and $del_dir eq 'Y') {
			$st = stat($_);
			if ($Cr_Mod eq "M") {
				if( ($now - $st->mtime)/3600/24 > $number_of_days and -d $_) {
					($DEBUG > 4) and write_log "CHECK> \t!!! Removing directory $new_dir/$_\n";
					rmdir $_;
				}
			}
			if ($Cr_Mod eq "C") {
				if( ($now - $st->ctime)/3600/24 > $number_of_days and -d $_) {
					($DEBUG > 4) and write_log "CHECK> \t!!! Removing directory $new_dir/$_\n";
					rmdir $_;
				}
			}
		}
	}
	($DEBUG > 2) and write_log "DEBUG> Returning back to $old_dir\n";
	chdir $old_dir;
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: remove_files - END\n";
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

write_log "\n";
write_log "################################################################################\n";
write_log "################################################################################\n";
write_log "###                                                                          ###\n";
write_log "###                                 S T A R T                                ###\n";
write_log "###                                                                          ###\n";
write_log "################################################################################\n";
write_log "################################################################################\n";

$TABS = "";

get_configuration;

if ($AFTERFLAG == 1) {
	get_Dttm_after;	# nacteni load_date LOAD_DATE
}
else {
	get_Dttm;	# nacteni load_date PREV_LOAD_DATE
}


write_log "################################################################################\n";
write_log "#                                  L O G                                       #\n";
write_log "################################################################################\n";

$dir = "BinLogs";
$current_dir = File::Spec->catfile($LOG_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($LOGS_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

 
$dir = "CmdLogs";
$current_dir = File::Spec->catfile($LOG_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($LOGS_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);


$dir = "SessLogs";
$current_dir = File::Spec->catfile($LOG_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($LOGS_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);


$dir = "ETLLogs";
$current_dir = File::Spec->catfile($LOG_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($LOGS_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);


$dir = "TDLogs";
$current_dir = File::Spec->catfile($LOG_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($LOGS_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);


$dir = "TDLogs/TPTLogs";
$current_dir = File::Spec->catfile($LOG_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($LOGS_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);


$dir = "WorkflowLogs";
$current_dir = File::Spec->catfile($LOG_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($LOGS_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);   

remove_files($LOGS_ARCHIVE_DIR, $LOGS_ARCHIVE_DIR, $LOGS_ARCHIVE_EXPIRATION_DAYS, 'Y', 'C');


write_log "################################################################################\n";
write_log "#                             E X T R A C T S                                  #\n";
write_log "################################################################################\n";

remove_files($EXTRACTS_ARCHIVE_DIR, $EXTRACTS_ARCHIVE_DIR, $EXTRACTS_ARCHIVE_EXPIRATION_DAYS, 'Y', 'C');

write_log "################################################################################\n";
write_log "#                             D A T A                                          #\n";
write_log "################################################################################\n";

$dir = "BadFiles";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "SrcFiles/SS";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "SrcFiles/Stg";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "SrcFiles/Tgt/OKMirr";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
copy_directory ($current_dir, $new_dir);

$dir = "SrcFiles/Tgt/EI";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "SrcFiles/StgWrk";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);
 
$dir = "SrcFiles/Tgt/ExpH";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Stg/Load";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/BadRecords";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/DQ";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/DQ/Load";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Stg";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Stg/Load";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Tgt";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Tgt/Load";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Dm";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Dm/Wrk";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Dm/Load";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);

$dir = "TgtFiles/Dm/Exp";
$current_dir = File::Spec->catfile($PMROOT_DIRECTORY , $dir);
$new_dir = File::Spec->catfile($DATA_ARCHIVE_DIR, $load_dttm, $dir);
move_directory ($current_dir, $new_dir);
 
remove_files($DATA_ARCHIVE_DIR, $DATA_ARCHIVE_DIR, $DATA_ARCHIVE_EXPIRATION_DAYS, 'Y', 'C');


write_log "################################################################################\n";
write_log "#                               R E M O V E                                    #\n";
write_log "################################################################################\n";

remove_files(File::Spec->catfile($PMROOT_DIRECTORY , "Security","Comps"), File::Spec->catfile($PMROOT_DIRECTORY , "Security","Comps"), 0, 'Y', 'C');
remove_files(File::Spec->catfile($PMROOT_DIRECTORY , "ParamFiles","Dynamic"), File::Spec->catfile($PMROOT_DIRECTORY , "ParamFiles","Dynamic"), 0, 'Y', 'C');
remove_files(File::Spec->catfile($PMROOT_DIRECTORY , "SrcFiles","Tgt","OKMirr","OKSplit"), File::Spec->catfile($PMROOT_DIRECTORY , "SrcFiles","Tgt","OKMirr","OKSplit"), 0, 'Y', 'C');


write_log "\n";
write_log "********************************************************************************\n";
write_log "***                             F I N I S H                                  ***\n";
write_log "********************************************************************************\n";
exit 0;

