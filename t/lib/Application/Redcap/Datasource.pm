package Redcap::Datasource;

use strict;
use warnings;

our $VERSION = 0.01;

use base qw(CohortExplorer::Datasource);
use CLI::Framework::Exceptions qw (:all);
use Exception::Class::TryCatch;

#-------

sub authenticate {

	my ( $self, $opts ) = @_;

	require SQL::Abstract::More;

	my $sqla = SQL::Abstract::More->new();

	my ( $stmt, @bind, $response );

	eval {
		( $stmt, @bind ) = $sqla->select(
			-columns => 'CONCAT( rp.project_id, \'.\', rur.data_export_tool )',
			-from    => [
				-join =>
				  qw(redcap_auth|ra <=>{username=username} redcap_user_rights|rur
				  <=>{project_id=project_id} redcap_projects|rp)
			],
			-where => {
				'rp.project_name'      => $self->name(),
				'rur.data_export_tool' => { '!=' => 0 },
				'ra.username'          => $opts->{username},
				'ra.password'          => \"= MD5 ( '$opts->{password}' )",
				'rp.project_id'        => \
'NOT IN ( SELECT project_id FROM redcap_external_links_exclude_projects )',
				-or => [
					{ 'rur.expiration' => \'<= CURDATE()' },
					{ 'rur.expiration' => undef }
				]
			}
		);
	};

	if ( catch my $e ) {
		throw_app_init_exception( error => $e );
	}

	eval { ($response) = $self->dbh()->selectrow_array( $stmt, undef, @bind ); };

	if ( catch my $e ) {
		throw_app_init_exception( error => $e );
	}

	return $response;
}

sub get_default_param {

	my ( $self, $opts, $response ) = @_;

	my $sqla = SQL::Abstract::More->new();

	my ( $default, $stmt, @bind );

	( $default->{project_id}, $default->{data_export_tool} ) = split '.',
	  $response;

	eval {
		( $stmt, @bind ) = $sqla->select(
			-columns => [qw/event_id form_name COUNT(form_name)|count/],
			-from    => 'redcap_events_forms',
			-where   => {
				'event_id' => \
" IN ( SELECT event_id FROM redcap_data where project_id = $default->{project_id} )"
			},
			-group_by => 'form_name',
			-having   => { 'count' => 1 }
		);
	};

	if ( catch my $e ) {
		throw_app_init_exception( error => $e );
	}

	eval {
		(
			$default->{static_table_count},
			$default->{static_tables},
			$default->{init_event_id}
		  )
		  = $self->dbh()->selectrow_array(
"SELECT COUNT( form_name ), GROUP_CONCAT( form_name ), MIN( event_id ) 
                                              FROM ( $stmt )`Table` GROUP BY count",
			undef, @bind
		  );
	};

	if ( catch my $e ) {
		throw_app_init_exception( error => $e );
	}

	if ( $default->{static_tables} ) {
		$default->{type} = 'longitudinal';
		$default->{static_tables} = [ split /,\s*/, $default->{static_tables} ];
	}

	else {
		$default->{type} = 'standard';
	}

	return $default;
}

sub get_entity_count_sql_param {

	my ( $self, $opts ) = @_;

	return {
		-columns => 'COUNT( DISTINCT record)',
		-from    => 'redcap_data',
		-where   => { 'project_id' => $self->project_id() }
	};

}

sub get_visit_max_sql_param {

	my ( $self, $opts ) = @_;

	return {
		-columns => 'COUNT(form_name)|count',
		-from    => 'redcap_events_forms',
		-where   => {
			'event_id' => \
" IN ( SELECT event_id FROM redcap_data where project_id = $self->project_id() )"
		},
		-group_by => 'form_name',
		-having   => { 'count' => { '>' => 1 } }
	};

}

sub get_tables_sql_param {

	my ( $self, $opts ) = @_;

	return {
		-columns => [
			"GROUP_CONCAT( DISTINCT form_name )|`Table`",
"GROUP_CONCAT( DISTINCT IF( form_menu_description IS NOT NULL, form_menu_description, '' ) SEPARATOR '')|`Label`",
			"COUNT( field_name )|`Variable_Count`"
		],
		-from  => 'redcap_metadata',
		-where => $self->data_export_tool() == 1
		? { 'project_id' => $self->project_id() }
		: {
			'project_id'   => $self->project_id(),
			'rm.field_phi' => { '=', undef },
		},
		-order_by => 'field_order',
		-group_by => 'form_name',
		-having   => { 'Variable_Count' => { '>', 0 } }
	};

}

sub get_variables_sql_param {

	my ( $self, $opts ) = @_;

	return {
		-columns => [
			"CONCAT(field_name, '.', form_name)|`Variable`", "field_name|`Name`",
			"form_name|`Table`",
"IF( element_validation_type IS NULL, 'text', element_validation_type)|`Type`",
			"field_units|`Unit`",
"IF( element_enum like '%, %', REPLACE( element_enum, '\\\\n', '\n'), '')|`Category`",
			"element_label|`Label`"
		],
		-from  => 'redcap_metadata',
		-where => $self->data_export_tool() == 1
		? { 'project_id' => $self->project_id() }
		: {
			'project_id'   => $self->project_id(),
			'rm.field_phi' => { '=', undef },
		},
		-order_by => 'field_order'
	};

}

sub set_schema {

	my ( $self, $opts ) = @_;

	my %schema = (
		-columns => {
			-identifier => 'rd.record',
			-table      => 'rm.form_name',
			-variable   => 'rd.field_name',
			-value      => 'rd.value'
		  } -from => [
			-join =>
			  qw(redcap_data|rd <=>{project_id=project_id} redcap_metadata|rm)
		  ],
		-where => { 'rd.project_id' => $self->project_id() }

	);

	( $schema{-columns}{-visit} = 'rd.event_id-' . $self->init_event_id() ) if ( $self->type() eq 'longitudinal' );

	return \%schema;

}

sub data_type_mapping {

	return {
		'int'                  => 'SIGNED',
		'float'                => 'DECIMAL',
		'date_dmy'             => 'DATE',
		'date_mdy'             => 'DATE',
		'date_ymd'             => 'DATE',
		'datetime_dmy'         => 'DATETIME',
		'datetime_mdy'         => 'DATETIME',
		'datetime_ymd'         => 'DATETIME',
		'datetime_seconds_dmy' => 'DATETIME',
		'datetime_seconds_mdy' => 'DATETIME',
		'datetime_seconds_ymd' => 'DATETIME',
		'number'               => 'DECIMAL',
		'number_1dp'           => 'DECIMAL',
		'number_2dp'           => 'DECIMAL',
		'number_3dp'           => 'DECIMAL',
		'number_4dp'           => 'DECIMAL',
		'time'                 => 'TIME',
		'time_mm_sec'          => 'TIME'
	};
}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Application::Opal::Datasource - Class to intialise datasource stored under RedCap framework see (L<http://project-redcap.org>)

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

The class is inherited from L<CohortExplorer::Datasource> class and overrides the following methods:

=head2

authenticate($self, $opts)

The authentication method authenticates the user and returns the project_id upon successful authentication.

=head2

get_default_param($self, $opts, $response)

This method returns a hash ref containing default parameters. The method determines the datasource type (i.e. standard or longitudinal) and static tables.

=head2

get_entity_count_sql_param($self, $opts)

The hash ref returned is used to retrieve the number of entities present in the datasource.

=head2

get_visit_max_sql_param($self, $opts)

The hash ref returned is used to retrieve the visit (max) for the longitudinal datasources.

=head 2

get_tables_sql_param($self, $opts)

The hash ref returned is used to retrieve the tables present in the datasource along with their attributes like entity_type, variable_count and label. 

=head 2

get_variables_sql_param($self, $opts)

The hash ref returned is used to retrieve the variables present in the datasource along with their attributes like type and categories (if any).


=head1 SEE ALSO

L<CohortExplorer>

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
