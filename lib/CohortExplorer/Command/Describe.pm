package CohortExplorer::Command::Describe;

use strict;
use warnings;

our $VERSION = 0.09;

use base qw(CLI::Framework::Command);
use CLI::Framework::Exceptions qw( :all );

#-------

sub usage_text {

	q{
            describe : show datasource description including entity count 
         };

}

sub validate {

	my ( $self, $opts, @args ) = @_;

	throw_cmd_validation_exception(
		error => 'Specified arguments when none required' )
	  if (@args);

}

sub run {

	my ( $self, $opts, @args ) = @_;

	my $cache = $self->cache->get('cache');

	eval 'require ' . ref $cache->{datasource};    # May or may not be preloaded

	my $tables = $cache->{datasource}->tables();

        # Get tables in the datasource with 'table' as the first column followed by table attributes
	my @columns = (
		'table',
		grep ( !/^table$/, keys %{ $tables->{ ( keys %$tables )[-1] } } )
	);

	push my @rows, \@columns;
	for my $table ( keys %$tables ) {
		push @rows,
		  [ map { $tables->{$table}{ $rows[0]->[$_] } } 0 .. $#{ $rows[0] } ];
	}

	print STDERR "Rendering datasource description ..." . "\n\n"
	  if ( $cache->{verbose} );

	return {
		headingText => $cache->{datasource}->name()
		  . ' datasource description ('
		  . $cache->{datasource}->entity_count()
		  . ' entities)',
		rows => \@rows
	};
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Describe - CohortExplorer class to describe the datasource

=head1 SYNOPSIS

B<describe>

B<d>

=head1 DESCRIPTION

The class is inherited from L<CLI::Framework::Command> and overrides the following hooks:

=head2  usage_text()

This method returns the usage information for the command.

=head2 validate( $opts, @args )

This method throws C<throw_cmd_validation_exception> exception imported from L<CLI::Framework::Exceptions> if an argument is supplied to this command because this command does not accept any arguments.

=head2 run( $opts, @args )

This method attempts to retrieve the table information and entity (count) from the datasource class and returns them to L<CohortExplorer::Application>.

=head1 DEPENDENCIES

L<CLI::Framework::Command>

L<CLI::Framework::Exceptions>

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Datasource>

L<CohortExplorer::Command::Find>

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
