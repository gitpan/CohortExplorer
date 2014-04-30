package CohortExplorer::Application::Opal::Version::2::Datasource;

use strict;
use warnings;

our $VERSION = 0.10;

use base qw(CohortExplorer::Datasource);
use Exception::Class::TryCatch;
use JSON qw( decode_json );

#-------

sub authenticate {

	my ( $self, $opts ) = @_;

	my $datasource_name = $self->name();

	require LWP::UserAgent;
	require MIME::Base64;

	# Authenticate using REST url
	# By default REST url is http://localhost:8080
	my $ua = LWP::UserAgent->new( timeout => 10 );
	my $url = $self->url() || 'http://localhost:8080';

	my $request =
	  HTTP::Request->new( GET => $url . "/ws/datasource/$datasource_name" );
	$request->header(
		Authorization => "X-Opal-Auth "
		  . MIME::Base64::encode( $opts->{username} . ':' . $opts->{password} ),
		Accept => "application/json"
	);
	my $response      = $ua->request($request);
	my $response_code = $response->code();

	if ( $response_code == 200 ) {
		my $decoded_json = decode_json( $response->decoded_content() );
		if ( $decoded_json->{type} ne 'mongodb' ) {

	                # Successful authentication returns tables and views accessible to the user
			my %views = map { $_ => 1 } @{ $decoded_json->{view} || [] };
			my @tables = @{ $decoded_json->{table} || [] };
			# Exclude all views
			my @tables_excluding_views =
			  defined $decoded_json->{view}
			  ? grep { not $views{$_} } @tables
			  : @tables;
			die "No tables but views found in $datasource_name\n"
			  unless (@tables_excluding_views);
			return \@tables_excluding_views;
		}
		else {
			die "Storage type for $datasource_name is not MySQL but MongoDB\n";
		}
	}

	elsif ( $response_code == 401 ) {
		return undef;
	}

	else {
		die  "Failed to connect to Opal server via $url (error $response_code)\n";
	}

}

sub default_parameters {

	my ( $self, $opts, $response ) = @_;

	my $datasource_name = $self->name();

	# By default,
	# datasource type is standard (i.e. non-longitudinal)
	# entity_type is participant
	# id_visit_separator is '_' (valid to longitudinal datasources only)
	my %default = (
		type        => $self->type()        || 'standard',
		entity_type => $self->entity_type() || 'Participant',
		allowed_tables => $response,
		username       => $opts->{username}
	);

	if ( $default{type} eq 'longitudinal' ) {

		$default{id_visit_separator} = $self->id_visit_separator() || '_';

                # Get static tables (if any) from datasource-config.properties and check them against @allowed_tables
		my @static_tables = ();
		my %table = map { $_ => 1 } @{ $default{allowed_tables} };
		$default{static_tables} = $self->static_tables() || undef;

		if ( $default{static_tables} ) {
			for ( split /,\s*/, $default{static_tables} ) {
				push @static_tables, $_ if ( $table{$_} );
			}
		}

		$default{static_tables} = \@static_tables;
	}

	else {
		$default{id_visit_separator} = undef;
		$default{static_tables}      = $default{allowed_tables};
	}

	# Get list of allowed variables from each table
	my $ua = LWP::UserAgent->new( timeout => 10 );
	my $url = $self->url() || 'http://localhost:8080';

	for my $table ( @{ $default{allowed_tables} } ) {
		my $request = HTTP::Request->new( GET => $url
			  . "/ws/datasource/$datasource_name/table/$table/entities" );
		$request->header(
			Authorization => "X-Opal-Auth "
			  . MIME::Base64::encode(
				$opts->{username} . ':' . $opts->{password}
			  ),
			Accept => "application/json"
		);
		my $response = $ua->request($request);

		# Get the first identifier
		if ( $response->code() == 200 ) {
			my $decoded_json = decode_json( $response->decoded_content() );
			$request =
			  HTTP::Request->new( GET => $url
				  . "/ws/datasource/$datasource_name/table/$table/valueSet/"
				  . ( $decoded_json->[0]->{identifier} || '' ) );
			$request->header(
				Authorization => "X-Opal-Auth "
				  . MIME::Base64::encode(
					$opts->{username} . ':' . $opts->{password}
				  ),
				Accept => "application/json"
			);

			$response = $ua->request($request);

			# Get all variables with accessible values
			if ( $response->code() == 200 ) {
				$decoded_json = decode_json( $response->decoded_content() );
				push @{ $default{allowed_variables} },
				  map { "$table.$_" } @{ $decoded_json->{variables} };
			}
		}
	}

	return \%default;
}

sub entity_structure {

	my ($self) = @_;

	my %struct = (

		-columns => {
			entity_id => "ve.identifier",
			variable  => "var.name",
			value     => "vsv.value",
			table     => "vt.name"
		},
		-from => [
			-join =>
			  qw/variable_entity|ve id=variable_entity_id value_set|vs <=>{value_table_id=id} value_table|vt <=>{vs.id=value_set_id} value_set_value|vsv <=>{vsv.variable_id=id} variable|var <=>{vt.datasource_id=id} datasource|ds/
		],
		-where => {
			've.type' => $self->entity_type(),
			'ds.name' => $self->name()
		}
	);

        # For longitudinal datasources split identifier into entity_id and visit using id_split_separator
	if ( $self->type() eq 'longitudinal' ) {
		my $id_visit_sep = $self->id_visit_separator();
		$struct{-columns}{entity_id} =
		  "SUBSTRING_INDEX( ve.identifier, '$id_visit_sep', 1)";

		# Check for the presence of id_visit_sep
		$struct{-columns}{visit} = "SUBSTRING_INDEX( ve.identifier, '$id_visit_sep', IF ( ve.identifier RLIKE '$id_visit_sep\[0-9\]+\$', -1, NULL ) )";
	}

	return \%struct;
}

sub table_structure {

	my ($self) = @_;

	return {

		-columns => {
			table          => "GROUP_CONCAT( DISTINCT vt.name)",
			variable_count => "COUNT( DISTINCT var.id)",
			entity_type    => "GROUP_CONCAT( DISTINCT vt.entity_type )"
		},
		-from => [
			-join =>
			  qw/value_table|vt <=>{vt.datasource_id=id} datasource|ds <=>{vt.id=value_table_id} variable|var =>{var.id=variable_id} variable_attributes|varatt/
		],
		-where => {
			'vt.entity_type' => $self->entity_type(),
			'vt.name'        => { -in => $self->allowed_tables() },
			'ds.name'        => $self->name()
		},
		-group_by => 'vt.id',
		-order_by => [qw/vt.name var.id var.variable_index/]
	};

}

sub variable_structure {

	my ($self) = @_;

	return {

		-columns => {

			variable => "GROUP_CONCAT( DISTINCT var.name )",
			table    => "GROUP_CONCAT( DISTINCT vt.name )",
			type     => "GROUP_CONCAT( DISTINCT var.value_type )",
			unit => "GROUP_CONCAT( DISTINCT IF( varatt.name = 'unitLabel' AND varatt.name IS NOT NULL, varatt.value, IF( var.unit IS NOT NULL, var.unit, '' )) SEPARATOR '')",
			category => "GROUP_CONCAT( DISTINCT CONCAT( cat.name, ', ', catatt.value ) SEPARATOR '\\n')",
			label => "GROUP_CONCAT( DISTINCT IF( varatt.name = 'label', varatt.value, '' ) SEPARATOR ' ')",
		},
		-from => [
			-join =>
			  qw/value_table|vt <=>{vt.datasource_id=id} datasource|ds <=>{vt.id=value_table_id} variable|var =>{var.id=variable_id} variable_attributes|varatt =>{var.id=variable_id} category|cat =>{id=category_id} category_attributes|catatt/
		],
		-where => {
			'vt.entity_type' => $self->entity_type(),
			'ds.name'        => $self->name(),
			"CONCAT( vt.name, '.', var.name )" => 
			{ -in => $self->allowed_variables() || [] },
		},
		-group_by => 'var.id',
		-order_by => [qw/vt.name var.id var.variable_index/]
	};

}

sub datatype_map {

	return {

		'integer'  => 'signed',
		'decimal'  => 'decimal',
		'date'     => 'date',
		'datetime' => 'datetime'
	};
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Application::Opal::Version::2::Datasource - CohortExplorer class to initialise datasource stored under L<Opal v2.0 (OBiBa)|http://obiba.org/node/63> SQL framework

=head1 SYNOPSIS

The class is inherited from L<CohortExplorer::Datasource> and overrides the following methods:

=head2 authenticate( $opts )

This method authenticates the user using the REST URL specified in C</etc/CohortExplorer/datasource-config.properties>. By default, the REST URL is C<http://localhost:8080>. The successful authentication results in the tables (excluding views) accessible to the user.

=head2 default_parameters( $opts, $response )

This method returns a hash ref containing all default parameters. The method also fetches a list of all variables whose values are accessible to the user via the REST API. By default,

  datasource type = standard (i.e. non-longitudinal), 
  entity_type = Participant, 
  id_visit_separator (longitudinal datasources) = _ 

=head2 entity_structure()

This method returns a hash ref defining the entity structure. The datasources in Opal are strictly standard but they can be easily made longitudinal by joining the C<entity_id> and C<visit> on C<id_visit_separator> (default C<_>). For example, PART001_1, implies the first visit of the participant PART001 and PART001_2 implies the second visit. The C<id_visit_separator> can also be a string (e.g. PARTIOP1, PARTIOP2).

=head2 table_structure()

This method returns a hash ref defining the table structure. The hash ref includes table attributes, C<variable_count>, C<label> and C<entity_type>.

=head2 variable_structure()

This method returns a hash ref defining the variable structure. The variable attributes are C<unit>, C<type>, C<category> and C<label>.

=head2 datatype_map()

This method returns variable type to SQL type mapping.

=head1 DEPENDENCIES

L<JSON>

L<LWP::UserAgent>

L<MIME::Base64>    

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
