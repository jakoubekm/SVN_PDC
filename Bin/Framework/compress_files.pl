# compress_files.pl

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  Name:         Compress_files.pl
#  Parameters:   DEBUG, BASE_DIRECTORY
#            
#  Project:      EDW Vodafone
#  Author:       Teradata - Petr Stefanek, Vladimir Duchon
#  Date:         2011-11-01
#-------------------------------------------------------------------------------
#  Description:  Script komprimuje soubory programu gzip
#-------------------------------------------------------------------------------
#  Modified:     
#  Date:         
#  Modification:  
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

#!E:/Perl/bin/perl -w

use Getopt::Long;
use Sys::Hostname;
use File::stat;
use File::Copy;
use File::Path;

GetOptions(
	"base_directory=s"=> \$BASE_DIRECTORY,
	"x=i"=> \$DEBUG
	);
if (not defined $DEBUG) { $DEBUG = 0; }
if ($DEBUG > 8) {
	#$DEBUG = 8; 			# don't comment this line from security reason
}
if (not defined $BASE_DIRECTORY) {
	print "USAGE: perl $0 -base_directory directory_name\n";
	exit 1;
}
($DEBUG > 4) and print "CHECK> BASE_DIRECTORY: $BASE_DIRECTORY\n";
($DEBUG > 4) and print "CHECK>          DEBUG: $DEBUG\n";

print "Unprocessed by Getopt::Long\n" if $ARGV[0];
foreach (@ARGV) {
	print "$_\n";
}

$| = 1; # vypnuti cache

################################################################################
# Konfigurovatelne parametry
#
$LOGFILE = File::Spec->catfile($ENV{"PMWorkflowLogDir"}, "BinLogs", "__Compress_files.log");
$RATIO = 5;					# kompresni pomer (1/faster/ - 9/better/)
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
# SUB compress_files - komprimuje soubory v adresari
################################################################################
sub compress_files {
	($DEBUG > 1) and write_log "$TABS>>>>>>>>>> SUB: compress_files - START\n";
	$TABS = $TABS . "    ";
	my $old_dir = shift;
	my $new_dir = shift;
	($DEBUG > 2) and write_log "DEBUG> Compressing files from $new_dir\n";
	chdir $new_dir;
	opendir(DIR, $new_dir);
	my @alldirs = readdir DIR;
	closedir DIR;
	foreach (@alldirs) {
		next if (substr($_,0,1) eq ".");
		if(-d $_) {
			compress_files($new_dir, File::Spec->catfile($new_dir , $_));
		}
		if( -f $_ and substr($_,-3,3) ne ".gz" ) {
			if( -f  $_ . ".gz") {
				@time = localtime(time);
				$year = 1900 + $time[5];
				$month = 1 + $time[4];
				$suffix = sprintf "_%4d%02d%02d%02d%02d%02d.gz", $year, $month, $time[3], $time[2], $time[1], $time[0];
				$cmd =  "gzip -S $suffix -" . $RATIO . " " . $_;
				system ($cmd);
			}
			else {
				$cmd =  "gzip -" . $RATIO . " " . $_;
				system ($cmd);
			}
		}
	}
	$DEBUG and print "Returning back to $old_dir\n";
	chdir $old_dir;
	$TABS = substr($TABS, 0, -4);
	($DEBUG > 1) and write_log "$TABS<<<<<<<<<< SUB: compress_files - END\n";
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

$TABS="";

write_log "\n";
write_log "################################################################################\n";
write_log "################################################################################\n";
write_log "###                                                                          ###\n";
write_log "###                                 S T A R T                                ###\n";
write_log "###                                                                          ###\n";
write_log "################################################################################\n";
write_log "################################################################################\n";

compress_files($BASE_DIRECTORY, $BASE_DIRECTORY);

write_log "\n";
write_log "********************************************************************************\n";
write_log "***                             F I N I S H                                  ***\n";
write_log "********************************************************************************\n";
exit 0;
