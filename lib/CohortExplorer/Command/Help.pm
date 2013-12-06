package CohortExplorer::Command::Help;
use base qw( CLI::Framework::Command::Meta );

use strict;
use warnings;

our $VERSION = 0.01;

#-------

sub usage_text {

       q{
              help [command]: usage information for an individual command or the application itself
        };
}

sub run {

	my ( $self, $opts, @args ) = @_;

	my $app = $self->get_app();    # metacommand is app-aware

	my $usage;
	my $command_name = shift @args;

	# Recognise help requests that refer to the target command by an alias
	my %alias = $app->command_alias();

	$command_name = $alias{$command_name}
	  if ( $command_name && exists $alias{$command_name} );

	my $h = $app->command_map_hashref();

	# First, attempt to get command-specific usage
	if ($command_name) {

		# (do not show command-specific usage message for non-interactive
		# commands when in interactive mode)
		$usage = $app->usage( $command_name, @args )
		  unless ( $app->get_interactivity_mode()
			&& !$app->is_interactive_command($command_name) );
	}

    # Commands search and compare can be invalid as they are application dependent (i.e. depend on
    # availability of variables and datasource type) where as, menu and console commands are invalid only
    # when the application is running in interactive mode

	# The application usage should only contain information on valid commands
	my $application_usage = $app->usage();

	for ( $app->noninteractive_commands() ) {
		$application_usage =~ s/\n\s+$_\s+\-[^\n]+//
		  if (
			(
				$_ =~ /^search|compare|history$/
				&& !$app->is_interactive_command($_)
			)
			|| ( $_ =~ /^menu|console$/ && $app->get_interactivity_mode() )
		  );

	}

	# Fall back to application usage message
	$usage ||= $application_usage;
	return $usage;
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Help - CLIF (see L<CLI::Framework::Command::Help>) built-in command to print application or command-specific usage messages. Only a small modification has been made to L<CLI::Framework::Command::Help> so that the application usage discards all sections appertaining to the invalid commands.

=cut
