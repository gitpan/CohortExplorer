package CohortExplorer::Command::Menu;

use strict;
use warnings;

our $VERSION = 0.01;

use base qw( CLI::Framework::Command::Menu );

#-------

sub menu_txt {

	my ($self) = @_;

	my $app = $self->get_app();

	# Build a list of valid and visible commands with aliases ...
	my ( @cmd, $txt );

	for my $cmd ( $app->get_interactive_commands() ) {

		push @cmd, $cmd
		  unless grep( /^$cmd$/, $app->noninteractive_commands() );

	}

	my %aliases = reverse $app->command_alias();
	for (@cmd) {
		$txt .= sprintf( "%-5s%2s%10s\n", $aliases{$_}, '-', $_ );
	}
	return "\n\n" . $txt . "\n\n";
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Menu - CLIF (see L<CLI::Framework::Command::Menu>) built-in command to show a command menu including the commands that are available to the running application. Only a small modification has been made to L<CLI::Framework::Command::Menu> so that the menu command only shows the valid and visible commands with aliases.

=cut
