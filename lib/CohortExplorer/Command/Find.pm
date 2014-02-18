package CohortExplorer::Command::Find;

use strict;
use warnings;

our $VERSION = 0.09;

use base qw(CLI::Framework::Command);
use CLI::Framework::Exceptions qw( :all );
use Exception::Class::TryCatch;

#-------

sub usage_text {

    q{
         find [--fuzzy|f] [--ignore-case|i] [--and|a] [keyword] : find variables using keywords

         
         By default, the arguments/keywords are joined using 'OR' unless option 'and' is specified.

         EXAMPLES
             
             find --fuzzy --ignore-case cancer diabetes	 (fuzzy and case insensitive search)

             find Demographics  (exact search)

             find -fi mmHg  (options with bundling and aliases)

             find -fia mmse total (using AND operation)

             
      
     };
}

sub option_spec {

    ( 
          [],
          [ 'ignore-case|i' => 'ignore case' ], 
          [ 'fuzzy|f' => 'fuzzy search' ],
          [],
          [ 'and|a' => 'Join keywords using AND (default OR)' ],
          []
    );
}

sub validate {

    my ( $self, $opts, @args ) = @_;

    throw_cmd_validation_exception(
        error => "At least one argument is required" )
      unless (@args);
}

sub run {

    my ( $self, $opts, @args ) = @_;
    my $datasource = $self->cache->get('cache')->{datasource};
    my $verbose    = $self->cache->get('cache')->{verbose};
    my $oper = $opts->{fuzzy} ? $opts->{ignore_case} ? -like : 'like binary' : -in;
    @args = $opts->{fuzzy} ? map { "\%$_%" } @args : @args;

        eval 'require ' . ref $datasource;    # May or may not be preloaded
    
        my ( $stmt, @bind, $sth );
        # Build a query to search variables based on keywords
        # Look for presence of keywords in -columns specified under $datasource->variable_structure method
        my $struct = $datasource->variable_structure();
        $struct->{$struct->{-group_by} ? -having : -where}{-or} = [
        map {
                {
                   $opts->{ignore_case} ? $_ : $_ => [ ( $opts->{'and'} ? '-and' : '-or' ) => map { { $oper => $_ } } @args ]
                }

            }
               $struct->{-group_by} ? map { "`$_`" } keys %{ $struct->{-columns} } : values %{ $struct->{-columns} }
        ];

        # Make sure 'variable' and 'table' are the first two columns followed by variable attributes
        my @columns = ( qw/table variable/, grep ( !/^(table|variable)$/, keys %{$struct->{-columns}} ) );
        $struct->{-columns} = [ map { $struct->{-columns}{$_} . "|`$_`" } @columns ];

        eval {
               ($stmt, @bind) = $datasource->sqla()->select(%$struct);
        };

    if ( catch my $e ) {
        throw_cmd_run_exception( error => $e );
    }

    eval {
        $sth = $datasource->dbh->prepare_cached($stmt);
        $sth->execute(@bind);
    };

    if ( catch my $e ) {
        throw_cmd_run_exception( error => $e );
    }

    push my @rows, ( $sth->{NAME}, @{ $sth->fetchall_arrayref( [] ) } )
      if ( $sth->rows );

    $sth->finish();

    if (@rows) {

        print STDERR "Found $#rows variable(s) matching the find query criteria ..."
          . "\n\n"
          . "Rendering variable description ..." . "\n\n"
          if ($verbose);

        return {
            headingText => 'variable description',
            rows        => \@rows
        };
    }

    else {
        print STDERR "Found 0 variable(s) matching the find query criteria ..."
          . "\n\n"
          if ($verbose);

        return undef;
    }

}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Find - CohortExplorer class to find variables using keywords

=head1 SYNOPSIS

B<find [OPTIONS] [KEYWORD]>

B<f [OPTIONS] [KEYWORD]>

=head1 DESCRIPTION

This class is inherited from L<CLI::Framework::Command> and overrides the following methods:

=head2 usage_text()

This method returns the usage information for the command.

=head2 option_spec() 

   ( 
     [ 'ignore-case|i' => 'ignore case'                  ], 
     [ 'fuzzy|f' => 'fuzzy search'                       ],
     [ 'and|a' => 'Join keywords using AND (default OR)' ] 
   )

=head2 validate( $opts, @args )

Validates the command options and arguments and throws exception if validation fails.

=head2 run( $opts, @args )

This method  enables the user to search variables using keywords. The command looks for the presence of keywords in the columns specified under L<variable_structure|CohortExplorer::Datasource/variable_structure()> method of the inherited datasource class. The command attempts to output the variable dictionary (i.e. meta data) of variables that are found. The variable dictionary can include the following variable attributes:

=over

=item 1

variable name (mandatory)

=item 2

table name (mandatory)

=item 3

type (i.e. integer, decimal, date, datetime etc.)

=item 4

unit

=item 5

categories (if any)

=item 6

label

=back

=head1 OPTIONS

=over

=item B<-f>, B<--fuzzy>

Fuzzy search

=item B<-i>, B<--ignore-case>

Ignore case

=item B<-a>, B<--and>

Join keywords using AND (default OR)

=back

=head1 DIAGNOSTICS

This command throws the following exceptions imported from L<CLI::Framework::Exceptions>:

=over

=item 1

C<throw_cmd_run_exception>: This exception is thrown if one of the following conditions are met,

=over

=item *

The C<select> method from L<SQL::Abstract::More> fails to construct the SQL from the supplied hash ref.

=item *

The method C<execute> from L<DBI> fails to execute the SQL query.

=back

=item 2

C<throw_cmd_validation_exception>: This exception is thrown only if no arguments (i.e. keywords) are supplied because this command requires at least one argument.

=back

=head1 DEPENDENCIES

L<CLI::Framework::Command>

L<CLI::Framework::Exceptions>

L<DBI>

L<Exception::Class::TryCatch>

L<SQL::Abstract::More>


=head1 EXAMPLES

 find --fuzzy --ignore-case cancer diabetes (fuzzy and case insensitive search)

 find Demographics (exact search)

 find -fi mmHg (options with bundling and aliases)

 find -fia mmse total (using AND operation)


=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Datasource>

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
