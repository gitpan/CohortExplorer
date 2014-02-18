package CohortExplorer::Application::REDCap::Datasource;

use strict;
use warnings;

our $VERSION = 0.09;

use base qw(CohortExplorer::Datasource);
use Exception::Class::TryCatch;

#-------

sub authenticate {

	my ( $self, $opts ) = @_;

	# Get the database handle and run authentication query
	# The user must have permisison to export data (i.e. export_data_tool != 0)

	my $stmt = "SELECT rp.project_id, rur.data_export_tool FROM redcap_auth AS ra INNER JOIN redcap_user_rights AS rur INNER JOIN redcap_projects AS rp ON rur.project_id=rp.project_id WHERE rp.project_name = ? AND rur.data_export_tool != 0 AND ra.username = ? AND ra.password = MD5(?) AND ( rp.project_id NOT IN ( SELECT project_id FROM redcap_external_links_exclude_projects ) AND ( rur.expiration <= CURDATE() OR rur.expiration IS NULL) )";

	my @bind = ( $self->name(), $opts->{username}, $opts->{password} );

        # Successful authentication outputs array_ref as response
	my $response = $self->dbh()->selectrow_arrayref( $stmt, undef, @bind );

	return $response;
}

sub default_parameters {

	my ( $self, $opts, $response ) = @_;

	my %default;

	# Add project_id and data_export_tool to the default parameter
	( $default{project_id}, $default{data_export_tool} ) = @$response;

	# Get static tables and init_event_id (dynamic tables) and visit_max
          my $stmt = "SELECT GROUP_CONCAT( if ( count = 1, form_name, NULL ) ) AS static_tables, SUBSTRING_INDEX( GROUP_CONCAT( DISTINCT IF(count > 1, event_id, NULL ) ), ',', 1) AS init_event_id, MAX( count ) AS visit_max FROM (SELECT MIN( event_id ) AS event_id, form_name, COUNT( form_name ) AS count FROM redcap_events_forms WHERE event_id IN ( SELECT event_id FROM redcap_events_metadata WHERE arm_id IN ( SELECT arm_id FROM redcap_events_arms WHERE project_id = ? )) GROUP BY form_name ) AS `table`";

         ( $default{static_tables}, $default{init_event_id}, $default{visit_max}) =
	   $self->dbh()->selectrow_array( $stmt, undef, $default{project_id} );

        # If the data was collated across multiple events the datasource is longitudinal
        # otherwise standard (i.e. non-longitudinal)
	if ( $default{init_event_id} && $default{visit_max} ) {
	     $default{type} = 'longitudinal';
	     $default{static_tables} = [ split /,\s*/, $default{static_tables} ];
	}

	else {
		$default{type} = 'standard';
	}

	return \%default;
}

sub entity_structure {

	my ($self) = @_;

	my %struct = (
		-columns => {
			entity_id => "rd.record",
			variable  => "rd.field_name",
			value     => "rd.value",
			table     => "rf.form_name"
		},
		-from => [
			-join => (
				$self->type() eq 'standard'
				? qw/redcap_data|rd <=>{project_id=project_id} redcap_metadata|rf/
				: qw/redcap_data|rd <=>{event_id=event_id} redcap_events_forms|rf/
			  )

		],
		-where => { 'rd.project_id' => $self->project_id() }
	);

	# Add visit column if the datasource is longitudinal
	$struct{-columns}{visit} = 'rd.event_id - ' . $self->init_event_id() . ' + 1'
	  if ( $self->type() eq 'longitudinal' );

	return \%struct;

}

sub table_structure {

	my ($self) = @_;

	return {
		-columns => {
			table => "GROUP_CONCAT( DISTINCT form_name )",
			label => "GROUP_CONCAT( DISTINCT IF( form_menu_description IS NOT NULL, form_menu_description, '' ) SEPARATOR '')",
			variable_count => "COUNT( field_name )"
		},
		-from  => 'redcap_metadata',
		-where => $self->data_export_tool() == 1
		? { 'project_id' => $self->project_id() }
		: {
			'project_id' => $self->project_id(),
			'field_phi'  => { '=', undef },
		},
		-order_by => 'field_order',
		-group_by => 'form_name',
		-having   => { 'variable_count' => { '>', 0 } }
	};

}

sub variable_structure {

	my ($self) = @_;

	# If data_export_tool is != 1 remove variables tagged as identifiers
	return {
		-columns => {
			variable => "field_name",
			table    => "form_name",
			type =>
"IF( element_validation_type IS NULL, 'text', element_validation_type)",
			unit => "field_units",
			category =>
"IF( element_enum like '%, %', REPLACE( element_enum, '\\\\n', '\n'), '')",
			label => "element_label"
		},
		-from  => 'redcap_metadata',
		-where => $self->data_export_tool() == 1
		? { 'project_id' => $self->project_id() }
		: {
			'project_id' => $self->project_id(),
			'field_phi'  => { '=', undef },
		},
		-order_by => 'field_order'
	};

}

sub datatype_map {

	return {

		'int'                  => 'signed',
		'float'                => 'decimal',
		'date_dmy'             => 'date',
		'date_mdy'             => 'date',
		'date_ymd'             => 'date',
		'datetime_dmy'         => 'datetime',
		'datetime_mdy'         => 'datetime',
		'datetime_ymd'         => 'datetime',
		'datetime_seconds_dmy' => 'datetime',
		'datetime_seconds_mdy' => 'datetime',
		'datetime_seconds_ymd' => 'datetime',
		'number'               => 'decimal',
		'number_1dp'           => 'decimal',
		'number_2dp'           => 'decimal',
		'number_3dp'           => 'decimal',
		'number_4dp'           => 'decimal',
		'time'                 => 'time',
		'time_mm_sec'          => 'time'

	};
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Application::REDCap::Datasource - CohortExplorer class to initialise datasource stored under L<REDCap|http://project-redcap.org/> framework

=head1 SYNOPSIS

The class is inherited from L<CohortExplorer::Datasource> and overrides the following methods:

=head2 authenticate( $opts )

This method authenticates the user by running the authentication query against the REDCap database. The successful authentication returns array ref containing C<project_id> and C<data_export_tool>. In order to use CohortExplorer the user must have the permission to export data in REDCap (i.e. C<data_export_tool != 0>).

=head2 default_parameters( $opts, $response )

This method adds C<project_id> and C<data_export_tool> to the datasource object as default parameters. Moreover, the method runs a SQL query to check if the datasource is standard or longitudinal. If the datasource is longitudinal then, C<static_tables>, C<visit_max> and C<init_event_id> are added as default parameters. At present the application does not support datasources with multiple arms.

=head2 entity_structure()

This method returns the hash ref defining the entity structure.

=head2 table_structure() 

This method returns the hash ref defining the table structure. The hash ref includes table attributes, C<variable_count> and C<label>.

=head2 variable_structure()

This method returns the hash ref defining the variable structure. The hash ref includes the condition appertaining to the inclusion/exclusion of the variables tagged as identifiers. The variable attributes include C<unit>, C<type>, C<category> and C<label>.

=head2 datatype_map()

This method returns variable type to SQL type mapping.

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Datasource>

L<CohortExplorer::Command::Describe>

L<CohortExplorer::Command::Find>

L<CohortExplorer::Command::History>

L<CohortExplorer::Command::Query::Search>

L<CohortExplorer::Command::Query::Compare>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2013 Abhishek Dixit (adixit@cpan.org). All rights reserved.

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
