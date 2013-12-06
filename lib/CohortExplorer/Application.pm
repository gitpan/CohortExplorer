package CohortExplorer::Application;

use strict;
use warnings;

our $VERSION = 0.01;

# Directory list for command-line completion
my @DIRS;

use base qw(CLI::Framework::Application);
use Carp;
use CLI::Framework::Exceptions qw ( :all);
use CohortExplorer::Datasource;
use File::Find;
use Exception::Class::TryCatch;
use Term::ReadKey;

#-------

sub usage_text {

	q{
                    CohortExplorer

                    OPTIONS
                            -d  --datasource  : provide datasource
                            -u  --username    : provide username
                            -p  --password    : provide password

                            -v  --verbose     : print with verbosity
                            -h  --help        : print usage message and exit
                            

                    COMMANDS
                             help      - show application or command-specific help
                             menu      - show menu of available commands
                             search    - search entities with/without condition(s) condition(s) on variable(s)
                             compare   - compare entities across visits (applicable only to longitudinal datasources)
                             describe  - show datasource description including entity count 
                             history   - show saved commands
                             find      - find variable(s) using keyword(s) 
                             console   - start a command console for the application
            
 };
}

sub option_spec {

	# Username, password and datasource name are mandatory options
	# Password may or may not be provided at start
	  [],
	  [ 'datasource|d:s' => 'provide datasource' ],
	  [ 'username|u:s'   => 'provide username' ],
	  [ 'password|p:s'   => 'provide password' ],
	  [],
	  [ 'verbose|v' => 'print with verbosity' ],
	  [ 'help|h'    => 'print usage message and exit' ],
	  []

}

sub validate_options {

	my ( $app, $opts ) = @_;

	# Show help and exit ...
	if ( $opts->{help} ) {
		$app->render( $app->get_default_usage() );
		exit;
	}

	else {
		if (   !$opts->{datasource}
			|| !$opts->{username}
			|| !exists $opts->{password} )
		{
			throw_app_opts_validation_exception(
				error => "Make sure all mandatory parameters are specified" );
		}
	}
}

sub command_map {

	console    => 'CLI::Framework::Command::Console',
	  help     => 'CohortExplorer::Command::Help',
	  menu     => 'CohortExplorer::Command::Menu',
	  describe => 'CohortExplorer::Command::Describe',
	  history  => 'CohortExplorer::Command::History',
	  find     => 'CohortExplorer::Command::Find',
	  search   => 'CohortExplorer::Command::Query::Search',
	  compare  => 'CohortExplorer::Command::Query::Compare'

}

sub command_alias {

	h      => 'help',
	  m    => 'menu',
	  s    => 'search',
	  c    => 'compare',
	  d    => 'describe',
	  hist => 'history',
	  f    => 'find',
	  sh   => 'console';
}

sub noninteractive_commands {

	my ($app) = @_;

	my $datasource = $app->cache->get('cache')->{datasource};

	eval 'require ' . ref $datasource;    # May or may not be preloaded

	# Menu and console commands are invalid under interactive mode
	push my @noninteractive_commands, qw/menu console/;

        # Search, compare and history commands require the user to have access to at least one variable
	push @noninteractive_commands, qw/search history compare/
	  unless ( keys %{ $datasource->variables() } );

	# Compare command is only available to longitudinal datasources
	push @noninteractive_commands, 'compare'
	  unless ( $datasource->type() eq 'longitudinal' );

	return @noninteractive_commands;
}

sub render {

	my ( $app, $output ) = @_;

	# Output from commands (not help) is hash with keys headingText and rows
	# headingText = a scalar with table heading
	# rows = ref to array of arrays
	if ( ref $output eq 'HASH' ) {
		require Text::ASCIITable;
		my $table = Text::ASCIITable->new(
			{
				hide_Lastline => 1,
				reportErrors  => 0,
				headingText   => $output->{headingText}
			}
		);

		my @cols = @{ shift @{ $output->{rows} } };

		# Format table based on the command output
		my $colWidth = $output->{headingText} eq 'command history' ? 1000 : 30;

		$table->setCols(@cols);

		for (@cols) {
			$table->setColWidth( $_, $colWidth );
		}

		for my $row ( @{ $output->{rows} } ) {
			my @row = map {
				substr( $_, ( $colWidth - 1 ), 0 ) = ' '
				  if ( $_ && $_ =~ /^[^\n]+$/ && length $_ >= $colWidth );
				$_
			} @$row;
			$table->addRow(@row);
		}

		delete @ENV{qw(PATH)};
		$ENV{PATH} = "/usr/bin:/bin";
		my $path = $ENV{'PATH'};

		open( my $less, '|-', $ENV{PAGER} || 'less', '-e' )
		  or croak "Failed to pipe to pager: $!\n";
		print $less "\n" . $table . "\n";
		close($less);
	}

	else {
		print STDERR $output;
	}

	return;
}

sub handle_exception {

	my ( $app, $e ) = @_;

	print $app->render( $e->description() . "\n\n" . $e->error() . "\n\n" );

	my $cache = $app->cache->get('cache');

	# Logs exceptions
	$cache->{logger}
	  ->error( $e->description() . ' [ User: ', $cache->{user} . ' ]' )
	  if ($cache);

	return;
}

sub pre_dispatch {

	my ( $app, $command ) = @_;

	my $cache = $app->cache->get('cache');

   # Search, compare and history commands are application dependent as they,
   # require the user to have access to at least one variable from table(s) and,
   # depend on the datasource type
	my @invalid_commands =
	  grep ( /^(search|compare|history)$/, $app->noninteractive_commands() );

	my $current_command = $app->get_current_command();

	# Invalid commands are not allowed to dispatch
	throw_invalid_cmd_exception(
		error => "Invalid command: " . $current_command . "\n" )
	  if ( grep( /^$current_command$/, @invalid_commands ) );

	# Log user activity
	$cache->{logger}
	  ->info( "Command '$current_command' is run by " . $cache->{user} );

}

sub read_cmd {

	my ($app) = @_;
	require Text::ParseWords;

	# Retrieve or cache Term::ReadLine object (this is necessary to save
	# command-line history in persistent object)
	my $term = $app->{_readline};
	unless ($term) {
		require Term::ReadLine;
		$term = Term::ReadLine->new('CohortExplorer');
		select $term->OUT;
		$app->{_readline} = $term;

		# Arrange for command-line completion
		my $attribs = $term->Attribs;
		$attribs->{completion_function} = $app->_cmd_request_completions();
	}

	# Prompt for the name of a command and read input from STDIN
	# Store the individual tokens that are read in @ARGV

	my $command_request =
	  $term->readline( '[' . $app->cache->get('cache')->{user} . ']$ ' );

	if ( !defined $command_request ) {

		# Interpret CTRL-D (EOF) as a quit signal
		@ARGV = $app->quit_signals();
		print "\n";    # since EOF character is rendered as ''
	}
	else {

		# Prepare command for usual parsing
		@ARGV = Text::ParseWords::shellwords($command_request);
		$term->addhistory($command_request)
		  if ( $command_request =~ /\S/ and !$term->Features->{autohistory} );
	}

	return 1;
}

sub _cmd_request_completions {

	my ($app) = @_;

	# Valid only when the application is running in console/interactive mode
	return sub {
		my ( $text, $line, $start ) = @_;
		my $datasource = $app->cache->get('cache')->{datasource};

		# Listen to search/compare commands
		if ( $line =~ /^\s*(search|compare|s|c)\s+/ ) {
			my $cmd = $1;

			# Make completion work with command aliases
			$cmd = 'search'  if ( $cmd eq 's' );
			$cmd = 'compare' if ( $cmd eq 'c' );

			# Ensure search/compare are valid commands
			unless ( !$app->is_interactive_command($cmd) ) {

				# Listen to 'output dir' option
				if ( $line =~ /(\-\-out=|\-o\s*)\'?$text$/ ) {
					return @DIRS;
				}

				# Listen to 'export' option
				elsif ( $line =~ /(\-\-export=|\-e\s*)\'?$text$/ ) {
					return keys %{ $datasource->tables() };
				}

				# Listen to arguments/condition (option)
				else {
					if ( $cmd eq 'search' ) {
						return keys %{ $datasource->variables() };
					}
					else {
						return (
							keys %{ $datasource->variables() },
							@{ $datasource->visit_variables() || [] }
						);
					}
				}
			}
		}

		# Listen to help command
		return grep( $_ ne 'help', $app->get_interactive_commands() )
		  if ( $line =~ /^\s*help/ );

		# Listen to describe and history commands
		return undef if ( $line =~ /^describe|history/ );

		# Default listening returns all interactive commands
		return grep( /^\s*$text/, $app->get_interactive_commands() )
		  if ( $start >= 0 );

	  }
}

sub init {

	my ( $app, $opts ) = @_;

	require Log::Log4perl;

	# Initialise logger
	eval { Log::Log4perl::init( $app->log_config_file() ); };

	if ( catch my $e ) {
		throw_app_init_exception( error => $e );
	}

	my $logger = Log::Log4perl->get_logger();

	$opts->{password} = $app->password_prompt() if ( $opts->{password} eq '' );

	# Initialise the datasource and store in cache for further use
	$app->cache->set(
		cache => {
			verbose    => $opts->{verbose},
			user       => $opts->{username} . '@' . $opts->{datasource},
			logger     => $logger,
			datasource => CohortExplorer::Datasource->initialise(
				$opts, $app->datasource_config_file()
			)
		}
	);

	if ( $app->get_current_command() eq 'console' ) {

		# No autocompletion if search is not a valid command
		if ( !grep( $_ eq 'search', $app->noninteractive_commands() ) ) {
			$app->find_sub_directories();
		}
		$app->render(
			"Welcome to the CohortExplorer version $VERSION console." . "\n\n"
			  . "Type 'help <COMMAND>' for command specific help." . "\n"
			  . "Use tab for command-line completion and ctrl + L to clear the screen."
			  . "\n"
			  . "Type q or exit to quit." );
	}

	return;
}

sub password_prompt {

    my ($app) = @_;

    $app->render("Enter password: ");
    ReadMode 'noecho';
    my $password = ReadLine(10);
    ReadMode 'normal';
    $app->render("\n");
    unless ($password) {
		$app->render("timeout\n");
		exit;
    }

    chomp $password;
    return $password;
}

sub find_sub_directories {

	my ($app) = @_;

	# Get all directories under /home/user for command-line completion
	no warnings 'File::Find';
	eval {
		find(
			{
				wanted => sub {
					push @DIRS, $_ if ( -d );
				},
				untaint  => 1,
				no_chdir => 1
			},
			$app->export_directory()
		);
	};

	if ( catch my $e ) {
		throw_app_init_exception( error => $e );

	}
}

sub log_config_file {

	return '/etc/CohortExplorer/log-config.properties';
}

sub datasource_config_file {

	return '/etc/CohortExplorer/datasource-config.properties';

}

sub export_directory {

	return '/home/' . getlogin();
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Application - CohortExplorer superclass

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

The class is inherited from L<CLI::Framework::Application> and overrides the following methods:

=head2 usage_text()

This method returns the application usage.

=head2 option_spec()

This method returns the application option specifications as expected by L<Getopt::Long::Descriptive>.

   ( 
     [ 'datasource|d:s' => 'provide datasource'           ],
     [ 'username|u:s'   => 'provide username'             ],
     [ 'password|p:s'   => 'provide password'             ],
     [ 'verbose|v'      => 'print with verbosity'         ],
     [ 'help|h'         => 'print usage message and exit' ] 
   )

=head2 validate_options( $opts )

This method ensures the user has supplied all mandatory options (i.e., datasource, username and password).

=head2 command_map()

This method returns the mapping between command names and command classes
 
  console  => 'CLI::Framework::Command::Console',
  help     => 'CohortExplorer::Application::Command::Help',
  menu     => 'CohortExplorer::Application::Command::Menu',
  describe => 'CohortExplorer::Application::Command::Describe',
  history  => 'CohortExplorer::Application::Command::History',
  find     => 'CohortExplorer::Application::Command::Find',
  search   => 'CohortExplorer::Application::Command::Query::Search',
  compare  => 'CohortExplorer::Application::Command::Query::Compare'

=head2 command_alias()

This method returns command alias

  h    => 'help',
  m    => 'menu',
  s    => 'search',
  c    => 'compare',
  d    => 'describe',
  hist => 'history',
  f    => 'find',
  sh   => 'console'

=head2 pre_dispatch( $command )

This method ensures the invalid commands do not dispatch and logs the commands dispatched by the users.

=head2 noninteractive_commands()

The method returns a list of the valid commands under interactive mode. The commands search, compare and history can be invalid as they are application dependent because they require the user to have access to at least one variable from the datasource and also depend on the datasource type (e.g., compare command is only available to longitudinal datasources).
 
=head2 render( $output )

This method is responsible for the presentation of the command output. All commands except help produce a tabular output.

=head2 read_cmd( )

This method attempts to provide the autocompletion of options and arguments wherever applicable.

=head2 handle_exception( $e )

This method prints and logs all exceptions.
 
=head2 init( $opts )

This method is responsible for the initialising of the application which includes initialising the logger and the datasource object. 

=head2 OPERATIONS

This class attempts to perform the following operations:

=over

=item 1

Initialises the application logger. The logger's configuration is read from the file specified under L<log_config_file|/log_config_file()>.

=item 2

Captures the application options and passes them along with the datasource configuration file specified under L<datasource_config_file|/datasource_config_file()> to the datasource class for object intialisation.

=item 3

Stores the resulting datasource object in the cache along with the logger object to be used by the commands.

=item 4

Creates a menu of available command based on the datasource type. For standard (i.e. non-longitudinal) datasource the command menu includes describe, find, search, history and help where as, the longitudinal datasources have also access to the compare command. The search, compare and history commands require the user to have access to at least one variable from the datasource.

=item 5

Provides autocompletion of command arguments/options (if applicable) for the user entered command. This feature is only available when the application is running in the console/interactive mode.

=item 6

Dispatches the command object for command specific processing.

=item 7

Captures the output returned by a command and displays them as a table.

=item 8 

Logs all exceptions thrown by the commands. 

=back

=head1 SUBCLASS HOOK

=head2 log_config_file()

Returns the full path to the log configuration file (default /etc/CohortExplorer/log-config.properties). The logger is implemented using L<Log::Log4perl>. The logger attempts to log both the error and information messages.

=head2 datasource_config_file()

Returns the full path to the datasource configuration file (default /etc/CohortExplorer/datasource-config.properties). To see how the datasources can be configured using the config file see L<CohortExplorer::Datasource>.

=head1 ERROR HANDLING

All exceptions thrown within CohortExplorer are treated by the C<handle_exception( $e )> method. The exceptions are imported from L<CLI::Framework::Exception>.
 
=head1 DEPENDENCIES

L<CLI::Framework::Application>

L<CLI::Framework::Exceptions>

L<Exception::Class::TryCatch>

L<File::Find>

L<Log::Log4perl>

L<Term::ReadKey>

L<Text::ASCIITable>

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Command::Describe>

L<CohortExplorer::Command::History>

L<CohortExplorer::Command::Query::Search>

L<CohortExplorer::Command::Query::Compare>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2013-2014 Abhishek Dixit (adixit@cpan.org). All rights reserved.

This program is free software: you can redistribute it and/or modify it under the terms of either:

=over

=item *
the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version, or

=item *
the "Artistic Licence".

=back

=head1 AUTHOR

Abhishek Dixit

=cut
