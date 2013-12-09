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

CohortExplorer::Command::Menu - CohortExplorer command to show a command menu

=head1 DESCRIPTION

This class is inherited from L<CLI::Framework::Command::Menu> and overrides the C<menu_txt()> method.

=head2 menu_txt()

This method creates a command menu including the commands that are available to the running application. Only a small modification has been made to the original code so that the menu includes command aliases along with the command names.

=cut
