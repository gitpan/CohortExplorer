package CohortExplorer::Datasource;

use strict;
use warnings;

our $VERSION = 0.03;

use Carp;
use Config::General;
use CLI::Framework::Exceptions qw ( :all);
use DBI;
use Exception::Class::TryCatch;
use SQL::Abstract::More;

#-------

sub initialise {

	my ( $class, $opts, $config_file ) = @_;

	my $param;

	# Get the configuration for the specified datasource from the config file
	eval {
		$param = {
			Config::General->new(
				-ConfigFile            => $config_file,
				-LowerCaseNames        => 1,
				-MergeDuplicateBlocks  => 1,
				-MergeDuplicateOptions => 1
			  )->getall()
		}->{datasource}{ $opts->{datasource} };
	};

	if ( catch my $e ) {
	      throw_app_init_exception( error => $e );
	}

	throw_app_init_exception(
		error => "Invalid datasource '$opts->{datasource}'" )
	  unless ($param);

	throw_app_init_exception( error =>
		  "Mandatory parameter namespace missing from '$opts->{datasource}'" )
	  unless ( $param->{namespace} );

        # Untaint
	$param->{namespace} =~ /^(.+)$/g;
        my $target_pkg = "CohortExplorer::Application::".ucfirst $1."::Datasource";

        $param->{name} ||= $opts->{datasource};
	$param->{alias}     = $opts->{datasource};
	$param->{dialect} ||= 'MySQL_old';

        eval "require $target_pkg";    # May or may not be preloaded

	eval {
		$param->{dbh} = DBI->connect( $param->{dsn}, $param->{username}, $param->{password},
			{ PrintError => 0, RaiseError => 1 } );
	};

	if ( catch my $e ) {
	     throw_app_init_exception( error => $e );
	}

	for (qw(dsn username password)) {
             # Remove DSN, username and password
             delete $param->{$_};
	}

	# Add sqla object
	$param->{sqla} = SQL::Abstract::More->new(
		sql_dialect    => $param->{dialect},
		max_members_IN => 100
	);

        # Instantiate datasource
	my $obj = $target_pkg->new($param) or croak "Failed to instantiate datasource package '$target_pkg' via new(): $!";

	$obj->_process($opts);
	return $obj;
}

sub _process {

	my ( $datasource, $opts ) = @_;

	print STDERR "Authenticating '$opts->{username}\@$opts->{datasource}' ...\n"
	  if ( $opts->{verbose} );

	my $response = $datasource->authenticate($opts);
		
	# Successful authentication returns a defined response
	throw_app_init_exception( error =>
		  "Failed to authenticate '$opts->{username}\@$opts->{datasource}'" )
	  unless ($response);

	print STDERR "Initializing application for '$opts->{username}\@$opts->{datasource}' ...\n"
	  if ( $opts->{verbose} );

	my $class = ref $datasource;

	my $default_param = $datasource->default_parameters( $opts, $response );

	throw_app_hook_exception( error =>
          "return from method 'default_parameters' in class $class is not hash worthy"
	) unless ( ref $default_param eq 'HASH' );

	for ( keys %$default_param ) {
		$datasource->{$_} = $default_param->{$_};
	}

	my $datasource_type = $datasource->type();

	throw_app_hook_exception(
		error => "Datasource is neither standard nor longitudinal" )
	  if ( !$datasource_type || $datasource_type !~ /^(standard|longitudinal)$/ );

	require Tie::IxHash;

	for my $p (qw/entity table variable/) {
		my $method = $p . '_structure';
		my $struct = $datasource->$method();

		# check all mandatory sql parameters are defined
		for (qw/-columns -from -where/) {
			throw_app_hook_exception(
				error => "'$_' missing in method '$method' of class '$class'" )
			  unless $struct->{$_};
		}

		throw_app_hook_exception( error =>
                      "'-columns' in method '$method' of class '$class' is not hash worthy"
		) unless ref $struct->{-columns} eq 'HASH';

		# Set entity params (i.e. entity_count, visit_max), tables and variables
		$method = 'set_' . $p . '_parameters';
		$datasource->$method($struct);
	}

	$datasource->set_visit_variables() if ( $datasource_type eq 'longitudinal' );

}

sub set_entity_parameters {

	my ( $datasource, $struct ) = @_;
	my $class = ref $datasource;

	# Make sure -columns hash in entity_structure has all mandatory keys
	for (
		$datasource->type() eq 'standard'
		? qw/entity_id table variable value/
		: qw/entity_id table variable value visit/
	  )
	{
		if ( $struct->{-columns}{$_} ) {
			if ( $_ eq 'entity_id' ) {
				$struct->{-columns}{$_} =
				  " COUNT( DISTINCT $struct->{-columns}{$_} ) ";
			}
			if ( $_ eq 'visit' ) {
				$struct->{-columns}{$_} =
				  " MAX( DISTINCT $struct->{-columns}{$_} + 0 ) ";
			}
		}
		else {
			throw_app_hook_exception( error =>
                         "Missing column '$_' in method 'entity_structure' of class '$class' "
			);
		}
	}

	# Retrieve entity_count and visit_max (if applicable)
	my ( $stmt, @bind );

	$struct->{-columns} =
	  [ $struct->{-columns}{entity_id}, $struct->{-columns}{visit} || 'NULL' ];

	eval { ( $stmt, @bind ) = $datasource->sqla()->select(%$struct); };

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	eval {
		( $datasource->{entity_count}, $datasource->{visit_max} ) =
		  $datasource->dbh()->selectrow_array( $stmt, undef, @bind );
	};

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	# Validate entity_count
	if ( $datasource->{entity_count} == 0 ) {
		throw_app_hook_exception(
			error => 'No entity found in datasource ' . $datasource->name() );
	}

	# Validate visit_max, only applicable to longitudinal datasources
	if ( $datasource->type() eq 'longitudinal'
		&& ( !$datasource->{visit_max} || $datasource->{visit_max} <= 1 ) )
	{
		throw_app_hook_exception(
			error => "Expecting visit (max) > 1 for a longitudinal datasource "
			  . $datasource->name() );
	}

}

sub set_table_parameters {

	my ( $datasource, $struct ) = @_;
	my $class           = ref $datasource;
	my $datasource_name = $datasource->name();

	throw_app_hook_exception( error =>
        "Missing column 'table' in method 'table_structure' of class '$class' "
	) unless ( $struct->{-columns}{table} );

	my ( $stmt, @bind, $sth );
	$struct->{-columns} =
	  [ map { $struct->{-columns}{$_} . "|`$_`" }
		  keys %{ $struct->{-columns} } ];

	# Retrieve data on tables
	eval { ( $stmt, @bind ) = $datasource->sqla()->select(%$struct); };

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	eval {
		$sth = $datasource->dbh()->prepare_cached($stmt);
		$sth->execute(@bind);
	};

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	my @rows = @{ $sth->fetchall_arrayref( {} ) };
	$sth->finish();

	throw_app_hook_exception(
		error => "No tables found in datasource $datasource_name" )
	  unless (@rows);

	tie %{ $datasource->{tables} }, "Tie::IxHash";    # Preserve order of tables

	for my $row (@rows) {
		throw_app_hook_exception( error =>
			  "Undefined table (name) found in datasource $datasource_name" )
		  unless ( $row->{table} );
		$datasource->{tables}{ $row->{table} } = $row;
	}
}

sub set_variable_parameters {

	my ( $datasource, $struct ) = @_;
	my $class           = ref $datasource;
	my $datasource_name = $datasource->name();

	# Check -columns hash has mandatory keys 'table' and 'variable'
	throw_app_hook_exception( error =>
       "Column variable/table missing from method variable_structure in class $class"
	) if ( !$struct->{-columns}{variable} || !$struct->{-columns}{table} );

	my ( $stmt, @bind, $sth );

	$struct->{-columns} =
	  [ map { $struct->{-columns}{$_} . "|`$_`" }
		  keys %{ $struct->{-columns} } ];

	eval { ( $stmt, @bind ) = $datasource->sqla()->select(%$struct); };

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	eval {
		$sth = $datasource->dbh()->prepare_cached($stmt);
		$sth->execute(@bind);
	};

	if ( catch my $e ) {
		throw_app_hook_exception( error => $e );
	}

	my @rows = @{ $sth->fetchall_arrayref( {} ) };
	$sth->finish();

	throw_app_hook_exception(
		error => "No variables found in datasource $datasource_name" )
	  unless (@rows);

	# Get the variable data type to sql data type mapping
	my $datatype_map = $datasource->datatype_map();

	throw_app_hook_exception( error =>
		  "return from method 'datatype_map' in class $class is not hash worthy"
	) unless ( ref $datatype_map eq 'HASH' );

	tie %{ $datasource->{variables} },
	  "Tie::IxHash";    # Preserve order of variables

	for my $row (@rows) {
		throw_app_hook_exception( error =>
			  "Undefined table/variable found in datasource $datasource_name" )
		  if ( !$row->{table} || !$row->{variable} );

                # Variables are referenced as 'Table.Variable'
                # $datasource->{variables} contains only two attributes (i.e. category and type) as
                # only these are used in search/compare commands.
                # Find command involves use of all variable attributes.
		$datasource->{variables}{ $row->{table} . '.' . $row->{variable} } = {
			'category' => $row->{category} || undef,

			# Convert variable_types to SQL types (default varchar(255))
			'type' => $datatype_map->{ $row->{type} } || 'CHAR(255)'
		};
	}
}

sub new {

	return bless $_[1], $_[0];
}

sub set_visit_variables {

	my ( $datasource ) = @_;

	my @static_tables = @{ $datasource->static_tables() || [] };
	my $visit_max = $datasource->visit_max();

	for my $var ( keys %{ $datasource->variables() } ) {
		$var =~ /^([^\.]+)\..+$/;

		unless ( grep( /^$1/, @static_tables ) ) {
			for ( qw(any last), 1 .. $visit_max ) {
				push @{ $datasource->{visit_variables} }, "V$_.$var";
			}
		}
	}
}

sub DESTROY {

	my ($datasource) = @_;

	$datasource->dbh()->disconnect() if ( $datasource->dbh() );

}

sub AUTOLOAD {

	my ($datasource) = @_;

	our $AUTOLOAD;

	( my $param = lc $AUTOLOAD ) =~ s/.*:://;

	return $datasource->{$param} || undef;
}

#--------- SUBCLASSES HOOKS --------#

sub authenticate { 1 }

sub default_parameters { {} }

sub entity_structure { }

sub table_structure { }

sub variable_structure { }

sub datatype_map { {} }

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Datasource - CohortExplorer datasource superclass

=head1 SYNOPSIS

    # The code below shows methods your datasource class overrides;

    package CohortExplorer::Application::REDCap::Datasource;
    use base qw( CohortExplorer::Datasource );

    sub authenticate { 
        
        my ($self, $opts) = @_;
                
        # authentication code...

          return $response
        
    }

    sub default_parameters {
        
         my ($self, $opts, $response) = @_;
          
         # get database handle (i.e. $self->dbh()) and run some SQL queries to get additional parameters
         # or, simply add some parameters without querying the database
         
         return $default;
    }
    
    sub entity_structure {
         
         my ($self) = @_;
         
         my %struct = (
                      -columns =>  {
                                     entity_id => "rd.record",
                                     variable => "rd.field_name",
                                     value => "rd.value",
                                     table => "rm.form_name"
                       },
                       -from =>  [ -join => qw/redcap_data|rd <=>{project_id=project_id} redcap_metadata|rm/ ],
                       -where =>  { 
                                     "rd.project_id" => $self->project_id()
                        }
          );

          $struct{-columns}{visit} =  'rd.event_id-' . $self->init_event_id()  
          if ( $self->type() eq 'longitudinal');
         
          return \%struct;
     }
     
         
    sub table_structure {
         
         my ($self) = @_;
         
         return {
                 
                  -columns => {
                                 table => "GROUP_CONCAT( DISTINCT form_name )", 
                                 variable_count => "COUNT( field_name )",
                                 label => "element_label"
                  },
                 -from  => "redcap_metadata",
                 -where => {
                             "project_id" => $self->project_id()
                  },
                 -order_by => "field_order",
                 -group_by => "form_name"
        };
     }
     
     sub variable_structure {
         
         my ($self) = @_;
         
         return {
                 -columns => {
                               variable => "field_name",
                               table => "form_name",
                               label => "element_label",
                               type => "IF( element_validation_type IS NULL, 'text', element_validation_type)",
                               category => "IF( element_enum like '%, %', REPLACE( element_enum, '\\\\n', '\n'), '')"
                 },
                -from => "redcap_metadata",
                -where => { 
                             "project_id" => $self->project_id()
                 },
                -order_by => "field_order"
        };
     }
     
     sub datatype_map {
        
      return {
                  'int'         => 'signed',
                 'float'        => 'decimal',
                 'date_dmy'     => 'date',
                 'date_mdy'     => 'date',
                 'date_ymd'     => 'date',
                 'datetime_dmy' => 'datetime'
      };
    }
    
=head1 OBJECT CONSTRUCTION

=head2 initialise( $opts, $config_file )

CohortExplorer::Datasource is an abstract factory; C<initialise()> is the factory method that constructs and returns an object of the datasource supplied as an application option. This class reads the datasource configuration from the config file (i.e. C</etc/CohortExplorer/datasource-config.properties>) to instantiate the datasource object. The config file takes the format below,

        <datasource Clinical> 
         namespace=Opal
         type=longitudinal
         static_tables=Demographics,FamilyHistory
         url=myhost
         dsn=DBI:mysql:database=opal;host=myhost;port=3306
         username=yourusername
         password=yourpassword
       </datasource> 

       <datasource Clinical1> 
         namespace=Opal
         type=longitudinal
         id_visit_separator=_
         name=Clinical
         url=myhost
         dsn=DBI:mysql:database=opal;host=myhost;port=3306
         username=yourusername
         password=yourpassword
       </datasource> 

       <datasource Drugs> 
         namespace=REDCap
         dsn=DBI:mysql:database=opal;host=myhost;port=3306
         username=yourusername
         password=yourpassword
       </datasource>

Each blocks holds a unique datasource configuration. Apart from some reserved parameters, C<namespace>, C<dsn>, C<username> and C<password> it is up to the user to decide what parameters they want to include in the configuration file. The user can specify the actual name of the datasource using the C<name> parameter provided the block name is an alias. If the C<name> parameter is not found then the block name is assumed to be the actual name of the datasource. In the example above, both Clinical and Clinical1 connect to the same datasource (i.e. Clinical) but with different configurations. Once this class has instantiated the datasource object, the user can access the parameters by simply calling the methods which have the same name as the parameters. For example, the database handle can be retrieved by C<$self-E<gt>dbh()> and id_visit_separator by C<$self-E<gt>id_visit_separator()>. The namespace is the name of the repository housing the datasource.

=head2 new()

    $object = $datasource_pkg->new();

Basic constructor.

=head1 PROCESSING

After instantiating the datasource object, the class first calls L<authenticate|/authenticate( $opts )> to perform the user authentication. If the authentication is successful (i.e. $response is defined), it sets the default parameters, if any ( via L<default_parameters|/default_parameters( $opts, $response )>). The subsequent steps include calling the methods, L<entity_structure|/entity_structure()>, L<table_structure|/table_structure()>, L<variable_structure|/variable_structure()>, L<datatype_map|/datatype_map()> and validating the return from each method. Upon successful validation the class attempts to set entity, table and variable specific parameters by invoking the methods below:

=head2 set_entity_parameters( $struct )

This method attempts to retrieve the entity parameters, C<entity_count> and C<visit_max> (for longitudinal datasources) from the repository. The method accepts the input from L<entity_structure|/entity_structure()>. 

=head2 set_table_parameters( $struct )

This method attempts to set the information on tables and their attributes as a hash where, table names are keys and attribute name-value pairs are hash values. The table attributes are read from the C<-columns> field specified under the hash ref from L<table_structure|/table_structure()>.

=head2 set_variable_parameters( $struct )

This method attempts to set the information on variables and their attributes as a hash where, keys are table and variable names joined by a dot and, values are the attribute name-value pairs. Instead of using the variable names as keys the method uses the combination of the table and the variable name as keys because,

=over

=item a. 

the resulting name also contains the name of the table, the variable was recorded under (e.g. CaseHistory.Onset_Age),

=item b.

distinguishes one variable from the other as sometimes variables from different tables may have the same name (e.g. Subject.Sex and Informant.Sex). 

=back

=head2 set_visit_variables()

This method is only called if the datasource is longitudinal. The method attempts to set the visit variables. The visit variables are only valid to dynamic tables and they represent the visit transformation of variables (e.g., V1.Var, V2.Var ... Vmax.Var, Vany.Var and Vlast.Var). The prefix C<V1> represents the first visit of the variable C<var>, C<V2> represents the second visit, C<Vany> implies any visit and C<Vlast> last visit. The L<compare|/CohortExplorer::Command::Query::Compare> command allows the use of visit variables when searching for entities of interest.

=head1 SUBCLASS HOOKS

The subclasses override the following hooks:

=head2 authenticate( $opts )

This method should return a response (a scalar) upon successful authentication otherwise return C<undef>. The method is called with one parameter, C<$opts> which is a hash with application options as keys and their user-provided values as hash values. B<Note> the methods below are only called if the authentication is successful.

=head2 default_parameters( $opts, $response )

This method should return a hash ref containing parameter name-value pairs. The user can run some SQL queries in case the parameters to be added to the datasource object first need to be retrieved from the database. The parameters used in calling this method are:
   
C<$opts> is a hash with application options as keys and their user-provided values as hash values.

C<$response> is the response received upon successful authentication. 

=head2 entity_structure()

The method should return a hash ref defining the entity structure in the database. The hash ref must have the following keys:

=over

=item B<-columns> 

C<entity_id>
 
C<variable> 

C<value>

C<table> 

C<visit> (only required for longitudinal datasources)

=item B<-from>

table specifications (see L<SQL::Abstract::More|SQL::Abstract::More/Table_specifications>)

=item B<-where> 

where clauses (see L<SQL::Abstract|SQL::Abstract/WHERE_CLAUSES>)

=back

=head2 table_structure()

The method should return a hash ref defining the table structure in the database. The C<table> in this context implies questionnaires or forms. For example,

      {
          -columns => {
                        table => "GROUP_CONCAT( DISTINCT form_name )", 
                        variable_count => "COUNT( field_name )",
                        label => "element_label"
          },
         -from  => "redcap_metadata",
         -where => {
                     "project_id" => $self->project_id()
         },
        -order_by => "field_order",
        -group_by => "form_name"

      }

the user should make sure the returned hash ref is able to produce the SQL output like the one below,

       +-------------------+-----------------+------------------+
       | table             | variable_count  | label            |
       +-------------------+-----------------+------------------+
       | demographics      |              26 | Demographics     |
       | baseline_data     |              19 | Baseline Data    |
       | month_1_data      |              20 | Month 1 Data     |
       | month_2_data      |              20 | Month 2 Data     |
       | month_3_data      |              28 | Month 3 Data     |
       | completion_data   |               6 | Completion Data  |
       +-------------------+-----------------+------------------+

B<Note> that C<-columns> hash ref must have the key C<table> corresponding to the name of form/questionnaire and others columns are table attributes. It is up to the user to decide what table attributes they think are suitable for the description of tables.

=head2 variable_structure()

This method should return a hash ref defining the variable structure in the database. For example,

         {
             -columns => {
                            variable => "field_name",
                            table => "form_name",
                            label => "element_label"
                            type => "IF( element_validation_type IS NULL, 'text', element_validation_type)",
                            category => "IF( element_enum like '%, %', REPLACE( element_enum, '\\\\n', '\n'), '')",
             },
            -from => "redcap_metadata",
            -where => { 
                        "project_id" => $self->project_id()
             },
             -order_by => "field_order"
         }

the user should make sure the returned hash ref is able to produce the SQL output like the one below,

       +---------------------------+---------------+-------------------------+---------------+----------+
       | variable                  | table         |label                    | category      | type     |
       +---------------------------+---------------+-------------------------+---------------------------
       | kt_v_b                    | baseline_data | Kt/V                    |               | float    |
       | plasma1_b                 | baseline_data | Collected Plasma 1?     | 0, No         | text     |
       |                           |               |                         | 1, Yes        |          |
       | date_visit_1              | month_1_data  | Date of Month 1 visit   |               | date_ymd |
       | alb_1                     | month_1_data  | Serum Albumin (g/dL)    |               | float    |
       | prealb_1                  | month_1_data  | Serum Prealbumin (mg/dL)|               | float    |
       | creat_1                   | month_1_data  | Creatinine (mg/dL)      |               | float    |
       +---------------------------+---------------+-----------+-------------------------------+--------+

B<Note> that C<-columns> hash ref must have the key C<variable> and C<table>. Again it is up to the user to decide what variable attributes (i.e. meta data) they think define the variables in the datasource. The categories in C<category> should be separated by newline.          
          
=head2 datatype_map()

This method should return a hash ref with variable type as keys and equivalent SQL type (i.e. castable) as value.

=head1 DIAGNOSTICS

=over

=item *

L<Config::General> fails to parse the datasource configuration file.

=item *

Failed to instantiate datasource package '<datasource pkg>' via new().

=item *

The return from methods C<default_parameters>, C<entity_structure>, C<table_structure>, C<variable_structure> and C<datatype_map> is either not hash worthy or incomplete.

=item *

The C<select> method from L<SQL::Abstract::More> fails to construct the SQL from the supplied hash ref.

=item *

The method C<execute> from L<DBI> fails to execute the SQL query.

=back

=head1 DEPENDENCIES

Carp

L<CLI::Framework>

L<Config::General>

L<DBI>

L<Exception::Class::TryCatch>

L<SQL::Abstract::More>

L<Tie::IxHash>

=head1 SEE ALSO

L<CohortExplorer>

L<CohortExplorer::Application::Opal::Datasource>

L<CohortExplorer::Application::REDCap::Datasource>

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
the " Artistic Licence ".

=back

=head1 AUTHOR

Abhishek Dixit

=cut
