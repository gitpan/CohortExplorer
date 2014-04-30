package CohortExplorer::Command::Query::Compare;

use strict;
use warnings;

our $VERSION = 0.10;

use base qw(CohortExplorer::Command::Query);
use CLI::Framework::Exceptions qw( :all );

#-------

sub usage_text {    # Command is only available to longitudinal datasources

	q\
                compare [--out|o=<directory>] [--export|e=<table>] [--export-all|a] [--save-command|s] [--stats|S] [--cond|c=<cond>]
                [variable] : compare entities across visits with/without conditions on variables


                NOTES
                   The variables Entity_ID and Visit (if applicable) must not be provided as arguments as they are already part of 
                   the query-set however, both can be used to impose conditions.

                   Other variables in arguments/cond (option) must be referenced as 'Table.Variable' or 'Visit.Table.Variable' where
                   visit can be Vany, Vlast, V1, V2, V3, etc.

                   The directory specified within the 'out' option must have RWX enabled (i.e. chmod 777) for CohortExplorer.

                   Conditions can be imposed using the operators: =, !=, >, <, >=, <=, between, not_between, like, not_like, in, not_in, 
                   regexp and not_regexp.

                   When conditions are imposed on variables with no prefix (e.g. V1, V2, Vany, Vlast etc) it is assumed that the
                   conditions apply to all visits of those variables.


                EXAMPLES
                   compare --out=/home/user/exports --stats --save-command --cond=V1.CER.Score="{'>','20'}" V1.SC.Date

                   compare --out=/home/user/exports --export=CER --cond=SD.Subject_Sex="{'=','Male'}" V1.CER.Score V3.DIS.Status

                   compare -o /home/user/exports -Ssa -c Vlast.CER.Score="{'in',['25','30','40']}" DIS.Status 

                   compare -o /home/user/exports -e CER -e SD -c Vlast.CER.Score="{'between',['25','30']}" DIS.Status

             \;
}

sub get_validation_variables {

	my ($self) = @_;

	my $datasource = $self->cache->get('cache')->{datasource};

	return [
		'Entity_ID',
		keys %{ $datasource->variables() },
		@{ $datasource->visit_variables() }
	];

}

sub get_query_parameters {

	my ( $self, $opts, $datasource, @args ) = @_;
	my $variables     = $datasource->variables();
	my @visits        = 1 .. $datasource->visit_max();
	my @static_tables = @{ $datasource->static_tables() || [] };
	my $struct        = $datasource->entity_structure();
	my %param;

        # Extract all variables from args/cond (option) except Entity_ID and Visit as they are dealt separately
	my @vars = grep( !/^(Entity_ID|Visit)$/,
		keys %{
			{
				map { $_ => 1 } map { s/^V(any|last|[0-9]+)\.//; $_ } @args,
				keys %{ $opts->{cond} }
			}
		  } );

	for my $var (@vars) {
		$var =~ /^([^\.]+)\.(.+)$/; 
		# Extract tables and variable names, a variable is referenced as 'Table.Variable'
		# Build a hash with keys 'static' and 'dynamic'
		# Each keys contains its own sql parameters
		my $table_type = grep ( /^$1$/, @static_tables ) ? 'static' : 'dynamic';
		push
		  @{ $param{$table_type}{-where}{ $struct->{-columns}{table} }{-in} },
		  $1;
		push @{ $param{$table_type}{-where}{ $struct->{-columns}{variable} }
			  {-in} }, $2;

		if ( $table_type eq 'dynamic' ) {

			# Each column corresponds to one visit
			for (@visits) {
				push @{ $param{$table_type}{-columns} },
                    " CAST( GROUP_CONCAT( IF( CONCAT( $struct->{-columns}{table}, '.', $struct->{-columns}{variable} ) = '$var'"
				  . " AND $struct->{-columns}{visit} = $_, $struct->{-columns}{value}, NULL)) AS "
				  . ( uc $variables->{$var}{type} )
				  . " ) AS `V$_.$var`";
			}
		}
		else {
			push @{ $param{$table_type}{-columns} },
			    " CAST( GROUP_CONCAT( DISTINCT "
			  . " IF( CONCAT( $struct->{-columns}{table}, '.', $struct->{-columns}{variable} ) = '$var', $struct->{-columns}{value}, NULL)) AS "
			  . ( uc $variables->{$var}{type} )
			  . " ) AS `$var`";
		}

		if ( $table_type eq 'static' ) {
			$param{$table_type}{-having}{"`$var`"} = eval $opts->{cond}{$var}
			  if ( $opts->{cond} && $opts->{cond}{$var} );
		}

		else {

	                # Build conditions for visit variables e.g. V1.Var, Vlast.Var, Vany.Var etc.
	                # Values inside array references are joined as 'OR' and hashes as 'AND'
			my @visit_vars = grep( /^(V(any|last|[0-9]+)\.$var|$var)$/,
				keys %{ $opts->{cond} } );

			for my $visit_var ( sort @visit_vars ) {

                                # Last visits (i.e. Vlast) for entities are not known in advance so practically any
                                # visit can be the last visit for any entity
				if ( $visit_var =~ /^Vlast\.$var$/ ) {
					my ( $opr, $val ) =
					  ( $opts->{cond}{"Vlast.$var"} =~
						  /^\{\'([^\']+)\',(.+)\}$/ );
					$val = !$2 ? undef : eval $2;

					if ( defined $param{$table_type}{-having}{-or} ) {
						map {
							${ $param{$table_type}{-having}{-or} }[$_]
							  ->{ "`V" . ( $_ + 1 ) . ".$var`" } =
							  { $opr => $val }
						} 0 .. $#{ $param{$table_type}{-having}{-or} };
					}
					else {
						$param{$table_type}{-having}{-or} = [
							map {
								{
									'Vlast'      => { -ident => $_ },
									"`V$_.$var`" => { $opr   => $val }
								}
							  } @visits
						];
					}
				}

				# Vany includes all visit variables joined as 'OR'
				elsif ( $visit_var =~ /^Vany\.$var$/ ) {
					my ( $opr, $val ) =
					  ( $opts->{cond}{"Vany.$var"} =~
						  /^\{\'([^\']+)\',(.+)\}$/ );
					$val = !$2 ? undef : eval $2;

					if ( defined $param{$table_type}{-having}{-and} ) {
						push @{ $param{$table_type}{-having}{-and} },
						  [ map { { "`V$_.$var`" => { $opr => $val } } }
							  @visits ];
					}

					else {
						$param{$table_type}{-having}{-and} = [
							{
								-or => [
									map { { "`V$_.$var`" => { $opr => $val } } }
									  @visits
								]
							}
						];
					}
				}

				# Individual visits (V1.var, V2.var, V3.var etc.)
				elsif ( $visit_var =~ /^V[0-9]{1,2}\.$var$/ ) {
					my $cond = eval $opts->{cond}{$visit_var};
					if ( $param{$table_type}{-having}{"`$visit_var`"} ) {
						$param{$table_type}{-having}{"`$visit_var`"} = [
							-and => $cond,
							[ -or => eval $opts->{cond}{$var}, { '=', undef } ]
						];
					}
					else {
						$param{$table_type}{-having}{"`$visit_var`"} = $cond;
					}
				}

                                # When a condition is imposed on a variable (with no prefix V1, Vlast, Vany)
                                # assume the condition applies to all visits of that variable (i.e. 'AND' case)
				else {

					map {
						$param{$table_type}{-having}{"`V$_.$var`"} =
						  [ ( eval $opts->{cond}{$var} ), { '=', undef } ]
					} @visits;

				}

			}
		}

	}

	for ( keys %param ) {
		if ( $_ eq 'static' ) {
			unshift @{ $param{$_}{-columns} },
			  $struct->{-columns}{entity_id} . '|`Entity_ID`';
		}

		else {

		        # Entity_ID and Visit are added to the list of SQL cols in dynamic param
			unshift @{ $param{$_}{-columns} },
			  (
				$struct->{-columns}{entity_id} . '|`Entity_ID`',
				'MIN( ' . $struct->{-columns}{visit} . ' + 0 )|`Vfirst`',
				'MAX( ' . $struct->{-columns}{visit} . ' + 0 )|`Vlast`',
				'GROUP_CONCAT( DISTINCT '
				  . $struct->{-columns}{visit}
				  . ')|`Visit`'
			  );
			$param{$_}{-having}{Visit} = eval $opts->{cond}{Visit}
			  if ( $opts->{cond} && $opts->{cond}{Visit} );
		}

		$param{$_}{-from} = $struct->{-from};
		$param{$_}{-where} =
		  $struct->{-where}
		  ? { %{ $param{$_}{-where} }, %{ $struct->{-where} } }
		  : $param{$_}{-where};
		$param{$_}{-group_by} = 'Entity_ID';
		$param{$_}{-having}{Entity_ID} = eval $opts->{cond}{Entity_ID}
		  if ( $opts->{cond} && $opts->{cond}{Entity_ID} );

		# Make sure condition on 'tables' has no duplicate placeholders
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

        # Header of the csv output must pay attention to args and variables on which the condition is imposed
        # Extract visit specific variables from the result-set based on the variables provided as args/cond (option).
        # For example, variables in args/cond variables are V1.Var and Vlast.Var but as the result-set contains all visits of
        # the variable 'var' so discard V2.var and V3.var and select V1.var and the equivalent Vlast.Var
	my $index = $result_set->[0][3] && $result_set->[0][3] eq 'Visit' ? 3 : 0;

	# Compiling regex to extract variables specified as args/cond (option)
	my $regex = join '|', map { s/^Vany\.//; $_ } @args,
	   keys %{ $opts->{cond} };

	$regex = qr/$regex/;

	my @index_to_use = sort { $a <=> $b } keys %{
		{
			map { $_ => 1 } (
				0 .. $index,
				grep( $result_set->[0][$_] =~ $regex,
					0 .. $#{ $result_set->[0] } )
			)
		}
	  };

	my @vars = @{ $result_set->[0] }[@index_to_use];

        # Extract last visit specific variables (i.e. Vlast.Var) in args/cond (option)
	my @last_visit_vars = keys %{
		{
			map { $_ => 1 }
			  grep( /^Vlast\./, ( @args, keys %{ $opts->{cond} } ) )
		}
	  };

	# Entities from the query are stored within a list
	my @result_entity;

	my $file = File::Spec->catfile( $dir, "QueryOutput.csv" );

	my $fh = FileHandle->new("> $file")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );
	my @cols = ( @vars, @last_visit_vars );
	$csv->print( $fh, \@cols )
	  or throw_cmd_run_exception( error => $csv->error_diag() );

	for my $row ( 1 .. $#$result_set ) {
		push @result_entity, $result_set->[$row][0];

		# Sort visits in the Visit column
		$result_set->[$row][3] =
		  join( ', ', ( sort { $a <=> $b } split ',', $result_set->[$row][3] ) )
		  if ( $index == 3 );

		my @last_visit_cols =
		  map { s/^Vlast\.//; "V$result_set->[$row][2].$_" } @last_visit_vars;

		my @last_visit_vals;

		for my $col (@last_visit_cols) {
			my ($index) =
			  grep { $result_set->[0][$_] eq $col } 0 .. $#{ $result_set->[0] };
			push @last_visit_vals, $result_set->[$row][$index];
		}

		my @vals = (
			( map { $result_set->[$row][$_] } @index_to_use ),
			@last_visit_vals
		);
		$csv->print( $fh, \@vals )
		  or throw_cmd_run_exception( error => $csv->error_diag() );
	}

	$fh->close();
	return \@result_entity;
}

sub process_table {

	my ( $self, $table, $datasource, $table_data, $dir, $csv, $result_entity ) =
	  @_;

	my @static_tables = @{ $datasource->static_tables() || [] };
	my $table_type =
	  $datasource->type() eq 'standard'
	  ? 'static'
	  : ( grep ( /^$table$/, @static_tables ) ? 'static' : 'dynamic' );

	# Extract the variables appertaining to the table from the variable list
	my @variables =
	  map { /^$table\.(.+)$/ ? $1 : () } keys %{ $datasource->variables() };

	my %data;

	# Get variables for static/dynamic tables
	my @header =
	    $table_type eq 'static'
	  ? @variables
	  : map {
		my $visit = $_;
		map { "V$visit.$_" } @variables
	  } 1 .. $datasource->visit_max();

	for (@$table_data) {

		# For static tables in longitudinal datasources table data comprise of
		# entity_id (0) variable (1) and value (2)
		# and in dynamic tables (longitudinal datasources only) it contains
		# entity_id, 'visit (3).variable (1)' and value (2)
		if ( $table_type eq 'static' ) {
			$data{ $_->[0] }{ $_->[1] } = $_->[2];
		}
		else {
			$data{ $_->[0] }{ 'V' . $_->[3] . '.' . $_->[1] } = $_->[2];
		}
	}

	# Write table data
	my $file = File::Spec->catfile( $dir, "$table.csv" );
	my $untainted = $1 if ( $file =~ /^(.+)$/ );
	my $fh = FileHandle->new("> $untainted")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );
	my @cols = ( qw(Entity_ID), @header );
	$csv->print( $fh, \@cols )
	  or throw_cmd_run_exception( error => $csv->error_diag() );

	# Write data for entities present in the result set
	for my $entity (@$result_entity) {
		my @vals = ( $entity, map { $data{$entity}{$_} } @header );
		$csv->print( $fh, \@vals )
		  or throw_cmd_run_exception( error => $csv->error_diag() );
	}

	$fh->close();
}

sub get_stats_data {

	my ( $self, $result_set ) = @_;
	my $index = $result_set->[0][3] && $result_set->[0][3] eq 'Visit' ? 3 : 0;
	my %data;

        # Remove visit suffix Vany, Vlast, V1, V2 etc. from the variables in the result-set (i.e. args/cond (option))
	my @vars = keys %{
		{
			map { s/^V(any|last|[0-9]+)\.//; $_ => 1 }
			  @{ $result_set->[0] }[ $index + 1 .. $#{ $result_set->[0] } ]
		}
	  };

	# Generate dataset for computing summary statistics from the result-set
	for my $row ( 1 .. $#$result_set ) {
		for my $var (@vars) {
			$data{ $result_set->[$row][0] }{$var} = [
				map {
					    $result_set->[0][$_] =~ /$var$/
					  ? $result_set->[$row][$_] || ()
					  : ()
				  } $index + 1 .. $#{ $result_set->[0] }
			];
			$data{ $result_set->[$row][0] }{'Visit'} =
			  [ split ',', $result_set->[$row][$index] ]
			  if ( $index != 0 );

		}
	}
	return ( \%data, 1, ( $index == 0 ? qw(Entity_ID) : qw(Entity_ID Visit) ),
		@vars );
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Query::Compare - CohortExplorer class to compare entities across visits

=head1 SYNOPSIS

B<compare [OPTIONS] [VARIABLE]>

B<c [OPTIONS] [VARIABLE]>

=head1 DESCRIPTION

The compare command enables the user to compare entities across visits. The user can also impose conditions on variables. Moreover, the command also enables the user to view summary statistics and export tables in csv format. The command is only available to longitudinal datasources.

This class is inherited from L<CohortExplorer::Command::Query> and overrides the following methods:

=head2 usage_text()

This method returns the usage information for the command.

=head2 get_validation_variables()

This method returns a ref to the list containing Entity_ID, all visit and non visit variables for validating arguments and condition option(s).

=head2 get_query_parameters( $opts, $datasource, @args )

This method returns a hash ref with keys, C<static>, C<dynamic> or C<both> depending on the variables supplied within arguments and conditions. The value of each key is a hash containing SQL parameters, C<-columns>, C<-from>, C<-where>, C<-group_by> and C<-having>.

=head2 process_result_set( $opts, $datasource, $result_set, $dir, $csv, @args )
     
This method writes result set to csv file and return a ref to the list containing C<Entity_IDs>.
        
=head2 process_table( $table, $datasource, $table_data, $dir, $csv, $result_entity )
        
This method writes the table data into a csv file for entities present in the result set. For static tables the csv contains C<Entity_ID> followed by variables' values and in case of dynamic tables the csv contains C<Entity_ID> followed by the values of all visit variables.

=head2 get_stats_data( $result_set )

This method returns a hash ref with C<Entity_ID> as keys and variable-value pairs as its value. The statistics in this command are computed with respect to the C<Entity_ID> and number of observation for each variable is equal to the number of times/visits each variable was recorded.
 
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
            
Impose conditions using the operators: C<=>, C<!=>, C<E<gt>>, C<E<lt>>, C<E<gt>=>, C<E<lt>=>, C<between>, C<not_between>, C<like>, C<not_like>, C<in>, C<not_in>, C<regexp> and C<not_regexp>.

=back

=head1 NOTES

The variables C<Entity_ID> and C<Visit> (if applicable) must not be provided as arguments as they are already part of the query-set. However, the user has the liberty to impose conditions on both the C<Entity_ID> and C<Visit>, using the C<cond> option. Other variables in arguments and conditions must be referenced as C<Table.Variable> or C<Visit.Table.Variable> where Visit = C<V1>, C<V2>, C<Vany>, C<Vlast> etc. When a condition is imposed on variables with no prefix C<V1>, C<V2>, C<Vany> or C<Vlast>, it is assumed that the condition applies to all visits of those variables. The directory specified within the C<out> option must have RWX enabled for CohortExplorer.

=head1 EXAMPLES

 compare --out=/home/user/exports --stats --save-command --cond=V1.CER.Score="{'>','20'}" V1.SC.Date

 compare --out=/home/user/exports --export=CER --cond=SD.Subject_Sex="{'=','Male'}" V1.CER.Score V3.DIS.Status

 compare -o /home/user/exports -Ssa -c Vlast.CER.Score="{'in',['25','30','40']}" DIS.Status 

 compare -o /home/user/exports -e CER -e SD -c Vlast.CER.Score="{'between',['25','30']}" DIS.Status

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
