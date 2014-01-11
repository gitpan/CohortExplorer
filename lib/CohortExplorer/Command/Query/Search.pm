package CohortExplorer::Command::Query::Search;

use strict;
use warnings;

our $VERSION = 0.05;

use base qw(CohortExplorer::Command::Query);
use CLI::Framework::Exceptions qw( :all );

#-------

sub usage_text {    # Command is available to both standard and longitudinal datasources

	q\
              search [--out|o=<directory>] [--export|e=<table>] [--export-all|a] [--save-command|s] [--stats|S] [--cond|c=<cond>] 
              [variable] : search entities with/without conditions on variables
              
              
              NOTES
                 The variables Entity_ID and Visit (if applicable) must not be provided as arguments as they are already part of
                 the query-set however, both can be used to impose conditions.

                 Other variables in arguments/cond (option) must be referenced as 'Table.Variable'.

                 The conditions can be imposed using the operators: =, !=, >=, >, <, <=, between, not_between, like, not_like, in, 
                 not_in and regexp.

                 The directory specified within the 'out' option must have RWX enabled for CohortExplorer.


              EXAMPLES
                 search --out /home/user/exports --stats --save-command --cond CER.Score="{'<=','30'}" SC.Date

                 search --out /home/user/exports --export-all --cond SD.Subject_Sex="{'=','Male'}" CER.Score DIS.Status

                 search -o /home/user/exports -e DS -e SD -c Entity_ID="{'like',['SUB100%','SUB200%']}" DIS.Status

                 search -o /home/user/exports -Ssa -c Visit="{'in',['1','3','5']}" DIS.Status 

                 search -o /home/user/exports -c CER.Score="{'between',['25','30']}" DIS.Status
 \;
}

sub get_query_parameters {

	my ( $self, $opts, $datasource, @args ) = @_;
	my $datasource_type = $datasource->type();
	my $variables       = $datasource->variables();
	my @static_tables   = @{ $datasource->static_tables() || [] };
	my $struct          = $datasource->entity_structure();
	my %param;
	my @vars_in_condition = grep ( !/^(Entity_ID|Visit)$/, keys %{ $opts->{cond} } );
	
	my %args = map { $_ => 1 } @args;
	my @vars = ( @args, grep { !$args{$_} } @vars_in_condition );

	for (@vars) {
		/^([^\.]+)\.(.+)$/; # Extract tables and variable names, a variable is referenced as 'Table.Variable'
		my $table_type =
		  $datasource_type eq 'standard'
		  ? 'static'
		  : ( grep ( /^$1$/, @static_tables ) ? 'static' : 'dynamic' );

                # Build a hash with keys 'static' and 'dynamic'.
                # Each key contains its own SQL parameters
                # In static tables the rows are grouped on Entity_ID where as in dynamic tables
                # (i.e. longitudinal datasources) the rows are grouped on Entity_ID and Visit
                push @{ $param{$table_type}{-where}{ $struct->{-columns}{table} }{-in} }, $1;
                
                push @{ $param{$table_type}{-where}{ $struct->{-columns}{variable} }{-in} }, $2;
		push @{ $param{$table_type}{-columns} },
		    " CAST( GROUP_CONCAT( "
		  . ( $table_type eq 'static' ? 'DISTINCT' : '' )
		  . (
" IF( CONCAT( $struct->{-columns}{table}, '.', $struct->{-columns}{variable} ) = '$_', $struct->{-columns}{value}, NULL ) ) AS "
		  )
		  . ( uc $variables->{$_}{type} )
		  . " ) AS `$_`";

		$param{$table_type}{-having}{"`$_`"} = eval $opts->{cond}{$_}
		  if ( $opts->{cond} && $opts->{cond}{$_} );
	}

	for ( keys %param ) {

		if ( $_ eq 'static' ) {
			unshift @{ $param{$_}{-columns} },
			  $struct->{-columns}{entity_id} . '|`Entity_ID`';
			$param{$_}{-group_by} = 'Entity_ID';
		}

		else {

		        # Entity_ID and Visit are added to the list of SQL cols in dynamic param
			unshift @{ $param{$_}{-columns} },
			  (
				$struct->{-columns}{entity_id} . '|`Entity_ID`',
				$struct->{-columns}{visit} . '|`Visit`'
			  );
			$param{$_}{-group_by} = [qw/Entity_ID Visit/];
			$param{$_}{-having}{Visit} = eval $opts->{cond}{Visit}
			  if ( $opts->{cond} && $opts->{cond}{Visit} );
		}

		$param{$_}{-from} = $struct->{-from};
		$param{$_}{-where} =
		  $struct->{-where}
		  ? { %{ $param{$_}{-where} }, %{ $struct->{-where} } }
		  : $param{$_}{-where};
		$param{$_}{-having}{Entity_ID} = eval $opts->{cond}{Entity_ID}
		  if ( $opts->{cond} && $opts->{cond}{Entity_ID} );

		# Make sure condition clause in 'tables' has no duplicate placeholders
		$param{$_}{-where}{ $struct->{-columns}{table} }{-in} = [
			keys %{
				{
					map { $_ => 1 }
					  @{ $param{$_}{-where}{ $struct->{-columns}{table} }{-in} }
				}
			  }
		];

	}

	return \%param;

}

sub process_result_set {

	my ( $self, $opts, $datasource, $result_set, $dir, $csv, @args ) = @_;

	my %result_entity;

	# Write result set
        my $file = File::Spec->catfile($dir, "QueryOutput.csv");
	my $fh = FileHandle->new("> $file")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	# Returns hash ref to hash with key as entity_id and value either:
	# list of visit numbers if the result-set contains visit column
	# (i.e. dynamic tables- Longitudinal datasources) or,
	# empty list (i.e. static tables)
	for ( 0 .. $#$result_set ) {
		if ( $_ > 0 ) {
			push @{ $result_entity{ $result_set->[$_][0] } },
			  $result_set->[0][1] eq 'Visit' ? $result_set->[$_][1] : ();
		}

		$csv->combine( @{ $result_set->[$_] } )
		  ? print $fh $csv->string() . "\n"
		  : throw_cmd_run_exception( error => $csv->error_input() );
	}

	$fh->close();

	return \%result_entity;

}

sub process_table {

	my ( $self, $table, $datasource, $table_data, $dir, $csv, $result_entity ) =
	  @_;
	my @static_tables = @{ $datasource->static_tables() || [] };
	my $table_type =
	  $datasource->type() eq 'standard'
	  ? 'static'
	  : ( grep ( /^$table$/, @static_tables ) ? 'static' : 'dynamic' );

	# Get table header
	my @variable =
	  map { /^$table\.(.+)$/ ? $1 : () } keys %{ $datasource->variables() };

	my %data;

	for (@$table_data) {

                # For static tables (i.e. standard/longitudinal) datasources table data comprise of
                # entity_id (0), variable (1) and value (2) and,
                # dynamic tables ( longitudinal datasources only) contain visit (3) in addition
		if ( $table_type eq 'static' ) {
			$data{ $_->[0] }{ $_->[1] } = $_->[2];
		}
		else {
			$data{ $_->[0] }{ $_->[3] }{ $_->[1] } = $_->[2];
		}
	}

	# Add Visit column to the header if the table is dynamic
	my $file = File::Spec->catfile($dir, "$table.csv");
	my $untainted = $1 if ( $file =~ /^(.+)$/ );
	my $fh        = FileHandle->new("> $untainted")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	$csv->combine(
		(
			( $table_type eq 'static' ? qw(Entity_ID) : qw(Entity_ID Visit) ),
			@variable
		)
	  )
	  ? print $fh $csv->string()
	  . "\n"
	  : throw_cmd_run_exception( error => $csv->error_input() );

	# Write data for entities present in the result set
	for my $entity ( sort keys %$result_entity ) {
		if ( $table_type eq 'static' ) {
			my @vals = ( $entity, map { $data{$entity}{$_} } @variable );
			$csv->combine(@vals)
			  ? print $fh $csv->string() . "\n"
			  : throw_cmd_run_exception( error => $csv->error_input() );
		}
		else {  # For dynamic tables
			for my $visit (
				  @{ $result_entity->{$entity} }
				? @{ $result_entity->{$entity} }
				: keys %{ $data{$entity} }
			  )
			{
				my @vals = (
					$entity, $visit,
					map { $data{$entity}{$visit}{$_} } @variable
				);
				$csv->combine(@vals)
				  ? print $fh $csv->string() . "\n"
				  : throw_cmd_run_exception( error => $csv->error_input() );
			}
		}
	}
	$fh->close();
}

sub get_stats_data {

	my ( $self, $result_set ) = @_;

	# If the result set contains visit column then the data is grouped
	# by visit (i.e. dynamic tables/longitudinal datasources)
	my $index = $result_set->[0][1] eq 'Visit' ? 1 : 0;
	my %data;

	for my $row ( 1 .. $#$result_set ) {
		my $key = $index == 0 ? 1 : $result_set->[$row][$index];
		for ( $index + 1 .. $#{ $result_set->[0] } ) {
			push @{ $data{$key}{ $result_set->[0][$_] } },
			  $result_set->[$row][$_] || ();
		}
	}
	return ( \%data, $index, splice @{ $result_set->[0] }, 1 );
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Query::Search - CohortExplorer class to search entities

=head1 SYNOPSIS

B<search [OPTIONS] [VARIABLE]>

B<s [OPTIONS] [VARIABLE]>

=head1 DESCRIPTION

The search command enables the user to search entities using variables of interest. The user can also impose conditions on the variables. Moreover, the command also enables the user to view summary statistics and export the tables in csv format. The command is available to both standard and longitudinal datasources.

This class is inherited from L<CohortExplorer::Command::Query> and overrides the following methods:

=head2 usage_text()

This method returns the usage information for the command.

=head2 get_query_parameters( $opts, $datasource, @args )

This method returns a hash ref with keys, C<static>, C<dynamic> or C<both> depending on the datasource type and variables supplied within arguments and conditions. The value of each key is a hash containing SQL parameters, C<-columns>, C<-from>, C<-where>, C<-group_by> and C<-having>.

=head2 process_result_set( $opts, $datasource, $result_set, $dir, $csv, @args ) 
        
This method returns a hash ref with keys as C<Entity_ID> and values either a list of visit numbers, if the result-set contains visit column (i.e. dynamic tables- longitudinal datasources), or empty list (i.e. static tables - standard/longitudinal datasources )

=head2 process_table( $table, $datasource, $table_data, $dir, $csv, $result_entity ) 
        
This method writes the table data into a csv file for entities present in the result set. For static tables the data includes C<Entity_ID> followed by variables' values where as, for dynamic tables it includes an additional column C<Visit>. The column headers for csv are retrieved by C<variables()> method of the datasource class. 

=head2 get_stats_data( $result_set )
        
This method returns a hash ref with C<Visit> as keys and variable-value hash as its value provided, at least one variable in the query-set belongs to the dynamic table. For all other cases it simply returns a hash ref with variable-value pairs.
  
=head1 OPTIONS

=over

=item B<-o> I<DIR>, B<--out>=I<DIR>

Provide directory to export data

=item B<-e> I<TABLE>, B<--export>=I<TABLE>

Export table by name

=item B<-a>, B<--export-all>

Export all tables

=item B<-s>, B<--save--command>

Save command

=item B<-S>, B<--stats>

Show summary statistics

=item B<-c> I<COND>, B<--cond>=I<COND>
            
Impose conditions using the operators: C<=>, C<!=>, C<E<gt>>, C<E<gt>=>, C<E<lt>>, C<E<lt>=>, C<between>, C<not_between>, C<like>, C<not_like>, C<in>, C<not_in> and C<regexp>.

=back

=head1 NOTES

The variables C<Entity_ID> and C<Visit> (if applicable) must not be provided as arguments as they are already part of the query-set. However, the user has the liberty to impose conditions on both C<Entity_ID> and C<Visit>, using the C<cond> option. The directory specified within the C<out> option must have RWX enabled for CohortExplorer.

=head1 EXAMPLES

 search --out /home/user/exports --stats --save-command --cond DS.Status="{'=','CTL'}" SC.Date

 search --out /home/user/exports --export-all --cond SD.Subject_Sex="{'=','Male'}" CER.Score DIS.Status

 search -o /home/user/exports -e DS -e SD -c Entity_ID="{'like',['SUB100%','SUB200%']}" DIS.Status

 search -o /home/user/exports -Ssa -c Visit="{'in',['1','3','5']}" DIS.Status 

 search -o /home/user/exports -c CER.Score="{'between',['25','30']}" DIS.Status

=head1 DIAGNOSTICS

This class throws C<throw_cmd_run_exception> exception imported from L<CLI::Framework::Exceptions> if L<Text::CSV_XS> fails to construct a csv string from the list containing variables' values.

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Datasource>

L<CohortExplorer::Command::Describe>

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
