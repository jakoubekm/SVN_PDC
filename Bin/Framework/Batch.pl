#-------------------------------------------------------------------------------
#  Name:		Batch.pl
#  IN_parameters:	-wait <seconds> -exit <exit_cd>
#  OUT_paramaters:	exit_cd
#  Called from:		Engine.pl
#  Calling:		NONE
#-------------------------------------------------------------------------------
#  Project:		PDC
#  Author:		Teradata - Petr Stefanek
#  Date:		2011-09-08
#-------------------------------------------------------------------------------
#  Version:		1.0
#-------------------------------------------------------------------------------
#  Description:		Script will sleep for <second> and then exits with <exit_cd>
#-------------------------------------------------------------------------------
#  Version:
#  Modified:
#  Date:
#  Modification:
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

use Getopt::Long;

GetOptions(
	"wait=i"=> \$SECOND,
	"exit=i"=> \$EXIT_CD
	);
if (not defined $SECOND) { $SECOND = 1; }
if (not defined $EXIT_CD) { $EXIT_CD = 0; }

print "Unprocessed by Getopt::Long\n" if $ARGV[0];
foreach (@ARGV) {
	print "$_\n";
}

sleep $SECOND;
exit $EXIT_CD;
