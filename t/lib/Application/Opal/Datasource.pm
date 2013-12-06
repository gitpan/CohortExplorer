package Opal::Datasource;

use strict;
use warnings;

our $VERSION = 0.01;

use base qw(CohortExplorer::Datasource);
use CLI::Framework::Exceptions qw (:all);
use Exception::Class::TryCatch;

#-------

sub authenticate {

    my ( $self, $opts ) = @_;

    # Authenticate using REST url
    require LWP::UserAgent;
    require MIME::Base64;

    my $ua = LWP::UserAgent->new( timeout => 10 );
    my $url = $self->url() || 'http://localhost:8080';

    my $request = HTTP::Request->new( GET => $url . '/ws/datasources' );
    $request->header( Authorization => "X-Opal-Auth " . MIME::Base64::encode( $opts->{username} . ':' . $opts->{password} ),
                  Accept => "application/json" );
    my $response        = $ua->request($request);
    my $datasource_name = $self->name();

    if (   $response->is_success()
            && $response->content() =~
/\{\"name\"\:\s*\"$datasource_name\",\"link\"\:\s*\"\/datasource\/$datasource_name\",\"table\":\s*\[([^\]]+)\],\"view\":\s*\[([^\]]+)\]/
      )
    {
                # Successful authentication returns tables accessible to the user
        my @tables = map { s/\"//g; $_ } split /,/, $1;
        my %views = map { s/\"//g; $_ => 1 } split /,/, $2;
        return [ grep { not $views{$_} } @tables ];
    }

    if ( $response->code() ) {
         throw_app_init_exception( error => $response->code() == 500 
                                               ? "Failed to connect to Opal server for authentication"
                                   : "Failed to authenticate '$opts->{username}\@$opts->{datasource}'" 
                                     );
    }

    return undef;

}

sub get_default_param {

    my ( $self, $opts, $response ) = @_;

    my $datasource_name = $self->name();
    my $default = {
                type        => $self->type()        || 'standard',
                entity_type => $self->entity_type() || 'Participant',
                id_visit_separator => $self->type() eq 'standard' ? undef : $self->id_visit_separator() || '_',
                allowed_tables => $response
                  };

    if ( $default->{type} eq 'standard' ) {
         return $default;
    }

    elsif ( $default->{type} eq 'longitudinal' ) {
                # Get static tables (if specified in datasource-config.properties) and check them against allowed_tables
        my %table = map { $_ => 1 } @{ $default->{allowed_tables} };
        $default->{static_tables} = $self->static_tables() || undef;
        if ( $default->{static_tables} ) {
             for ( split /,\s*/, $default->{static_tables} ) {
               unless ( $table{$_} ) {
                    throw_app_init_exception( error => "'$_' is not a valid table in the datasource $datasource_name" );
               }
             }
        }

        $default->{static_tables} = [ split /,\s*/, $default->{static_tables} ] if ( $default->{static_tables} );
        return $default;
    }

    else {
           throw_app_init_exception( error => "'$default->{type}' is not a valid datasource type in datasource $datasource_name" );
    }

}

sub get_entity_count_sql_param {

    my ( $self, $opts ) = @_;

    my $id_visit_sep = $self->id_visit_separator();

    return {
        -columns => $self->type() eq 'standard' ? "COUNT( DISTINCT ve.identifier)" : "COUNT( DISTINCT SUBSTRING_INDEX( ve.identifier, '$id_visit_sep', 1) )",
        -from => [ -join => qw(variable_entity|ve id=variable_entity_id value_set|vs <=>{value_table_id=id} value_table|vt <=>{vt.datasource_id=id} datasource|ds) ],
        -where => {
                've.type' => $self->entity_type(),
                'ds.name' => $self->name(),
                'vt.name' => { -in => $self->allowed_tables() }
        }
    };

}

sub get_visit_max_sql_param { # Valid to longitudinal datasources only

    my ( $self, $opts ) = @_;

    my $id_visit_sep = $self->id_visit_separator();

    return {
        -columns =>  "MAX( DISTINCT CAST( SUBSTRING_INDEX ( ve.identifier, '$id_visit_sep', -1) AS SIGNED ) )",
        -from => [ -join => qw(variable_entity|ve id=variable_entity_id value_set|vs <=>{value_table_id=id} value_table|vt <=>{vt.datasource_id=id} datasource|ds) ],
        -where => {
                 've.type'       => $self->entity_type(),
                 'ds.name'       => $self->name(),
                 'vt.name'       => { -in => $self->allowed_tables() },
                 've.identifier' => { 'regexp' => "\[^$id_visit_sep\]\+\[0-9]+\$" }
        }
    };

}

sub get_tables_sql_param {

    my ( $self, $opts ) = @_;

    return {
        -columns => [ "GROUP_CONCAT( DISTINCT vt.name)|`Table`", "GROUP_CONCAT( DISTINCT IF(varatt.name IN ('description', 'info', 'source'), varatt.value, '') SEPARAToR '' )|`Label`", "COUNT( DISTINCT var.id)|`Variable_Count`", "GROUP_CONCAT( DISTINCT vt.entity_type )|`Entity_Type`"   ],
        -from => [ -join => qw(value_table|vt <=>{vt.datasource_id=id} datasource|ds <=>{vt.id=value_table_id} variable|var =>{var.id=variable_id} variable_attributes|varatt) ],
        -where => {
                'vt.entity_type' => $self->entity_type(),
                'vt.name'        => { -in => $self->allowed_tables() },
                'ds.name'        => $self->name()
        },
        -group_by => 'vt.id',
        -order_by => [qw(vt.name var.id var.variable_index)]
    };

}

sub get_variables_sql_param {

    my ( $self, $opts ) = @_;

    my $datasource_name = $self->name();

        # Administrator has access to all variables and tables in the datasource. For users other than the administrator only variables
        # with permissions TABLE_ALL, TABLE_VALUES, ADMINISTRATE and VIEW_ALL are retrieved
    my $acl = $opts->{username} eq 'administrator'
             ? { 'vt.name' => { -in => $self->allowed_tables() } }
             : {
             'vt.name' => [
              -and => { -in => $self->allowed_tables() },
            {
                  -in => \ "SELECT DISTINCT IF(permission IN ('TABLE_ALL', 'TABLE_VALUES', 'ADMINISTRATE', 'VIEW_ALL'), REPLACE(node, '/datasource/$datasource_name/table/', ''), '') 
                                    FROM subject_acl WHERE principal = '$opts->{username}' AND permission IN ('TABLE_ALL', 'TABLE_VALUES', 'VIEW_ALL', 'ADMINISTRATE')"
            }
        ],
        "CONCAT(vt.name,'.',var.name)" => {
                                        -in => \"SELECT DISTINCT IF(permission = 'VARIABLE_READ', REPLACE (node, '/datasource/$datasource_name/table/', ''), '') FROM subject_acl WHERE principal = '$opts->{username}' AND permission = 'VARIABLE_READ'"
        }
      };

    return {
        -columns => [  
                               "GROUP_CONCAT( DISTINCT CONCAT(vt.name, '.', var.name) )|`Variable`", 
                               "GROUP_CONCAT( DISTINCT var.name )|`Name`",
                   "GROUP_CONCAT( DISTINCT vt.name )|`Table`",
                   "GROUP_CONCAT( DISTINCT var.value_type )|`Type`",
                               "GROUP_CONCAT( DISTINCT IF( varatt.name = 'unitLabel' AND varatt.name IS NOT NULL, varatt.value, IF( var.unit IS NOT NULL, var.unit, '' )) SEPARATOR '')|`Unit`",
                               "GROUP_CONCAT( DISTINCT CONCAT( cat.name, '=', catatt.value ) SEPARATOR '\n ')|`Category`",
                               "GROUP_CONCAT( DISTINCT IF( varatt.name = 'label', varatt.value, '' ) SEPARATOR ' ')|`Label`",
         ],
        -from => [ -join => qw(value_table|vt <=>{vt.datasource_id=id} datasource|ds <=>{vt.id=value_table_id} variable|var =>{var.id=variable_id} variable_attributes|varatt =>{var.id=variable_id} category|cat =>{id=category_id} category_attributes|catatt) ],
        -where => {
                'vt.entity_type' => $self->entity_type(),
                'ds.name'        => $datasource_name,
                'varatt.name'    => { -in => [qw(questionnaire description stage label info source)]
               },
               %$acl
        },
        -group_by => 'var.id',
        -order_by => [qw(vt.name var.id var.variable_index)]
    };

}


sub register_schema {


       my ($self, $opts ) = @_;

       my %schema = (
                       -columns => {
                                      -table => 'vt.name',
                                      -variable => 'var.name',
                                      -value => 'vsv.value'
                                   }
                       -from => [ -join => qw(variable_entity|ve id=variable_entity_id value_set|vs <=>{value_table_id=id} value_table|vt <=>{vs.id=value_set_id} value_set_value|vsv <=>{vsv.variable_id=id} variable|var <=>{vt.datasource_id=id} datasource|ds) ],
                       -where => { 
                                   've.type' => $self->entity_type(),
                                   'ds.name' => $self->name()
                                 }

                     );


          if ($self->type() eq 'standard') {
              $schema{-columns}{-identifier} = 've.identifier';
          }

          else {
                  $schema{-columns}{-identifier} = 'SUBSTRING_INDEX(ve.identifier, '.$self->id_visit_separator(), ', 1)';
                  $schema{-columns}{-visit} = 'SUBSTRING_INDEX(ve.identifier, '.$self->id_visit_separator(), ', -1)';
          }

          return \%schema;

}


sub data_type_mapping {

           return {
            'integer'  => 'SIGNED',
            'decimal'  => 'DECIMAL',
            'date'     => 'DATE',
            'datetime' => 'DATETIME'
              };


#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Application::Opal::Datasource - Class to initialise datasource stored under Opal SQL framework see (L<http://obiba.org/node/63>)

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

The class is inherited from L<CohortExplorer::Datasource> class and overrides the following methods:

=head2

authenticate($self, $opts)

The authentication method authenticates the user using the REST URL specified in datasource-config.properties.
By default the REST URL is assumed to be http://localhost:8080. The successful authentication results in tables (excluding views) accessible to the user.

=head2

get_default_param($self, $opts, $response)

This method returns a hash ref to hash containing default parameters with their values. For example, by default, 
datasource type = standard (i.e. non-longitudinal), 
entity_type = Participant, 
id_visit_separator (longitudinal datasources) = _

=head2

get_entity_count_sql_param($self, $opts)

The hash ref returned is used to retrieve the number of entities present in the datasource.

=head2

get_visit_max_sql_param($self, $opts)

The hash ref returned is used to retrieve the visit (max) for the longitudinal datasources.

=head 2

get_tables_sql_param($self, $opts)

The hash ref returned is used to retrieve the tables present in the datasource along with their attributes like entity_type, variable_count and label. The hash ref retrieves tables only accessible to the user. 

=head 2

get_variables_sql_param($self, $opts)

The hash ref returned is used to retrieve the variables present in the datasource along with their attributes like type and categories (if any). The hash ref retrieves variables only accessible to the user. 


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