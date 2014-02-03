package CohortExplorer::Command::Query;

use strict;
use warnings;

our $VERSION = 0.07;
our ( $COMMAND_HISTORY_FILE, $COMMAND_HISTORY_CONFIG, $COMMAND_HISTORY );
our @EXPORT_OK = qw($COMMAND_HISTORY);
my $ARG_MAX = 50;

#-------

BEGIN {
        use base qw(CLI::Framework::Command Exporter);
        use CLI::Framework::Exceptions qw( :all );
        use CohortExplorer::Datasource;
        use Exception::Class::TryCatch;
        use FileHandle;
        use File::HomeDir;
        use File::Spec;
        use Config::General;

	$COMMAND_HISTORY_FILE = $1 if ( File::Spec->catfile(File::HomeDir->my_home(), ".CohortExplorer_History") =~ /^(.+)$/ );
	
	my $fh = FileHandle->new( ">> $COMMAND_HISTORY_FILE" );
        throw_cmd_run_exception( error => "Make sure $COMMAND_HISTORY_FILE exists with RW enabled (i.e. chmod 766) for CohortExplorer" ) unless ($fh);
        $fh->close();

	eval {
		$COMMAND_HISTORY_CONFIG = Config::General->new(
			-ConfigFile            => $COMMAND_HISTORY_FILE,
			-MergeDuplicateOptions => "false",
			-StoreDelimiter        => "=",
			-SaveSorted            => 1
		);
	};

	if ( catch my $e ) {
	     throw_cmd_run_exception( error => $e );
	}

	$COMMAND_HISTORY = { $COMMAND_HISTORY_CONFIG->getall() };

}

sub option_spec {

	(
		[],
		[ 'cond|c=s%'      => 'impose conditions'         ],
		[ 'out|o:s'        => 'provide output directory'  ],
		[ 'save-command|s' => 'save command'              ],
		[ 'stats|S'        => 'show summary statistics'   ],
		[ 'export|e=s@'    => 'export tables by name'     ],
		[ 'export-all|a'   => 'export all tables'         ],
		[]
	);
}

sub validate {

	my ( $self, $opts, @args ) = @_;

	my $cache = $self->cache->get('cache');

	eval 'require ' . ref $cache->{datasource};    # May or may not be preloaded

	print STDERR "Validating command options/arguments ..." . "\n\n"
	  if ( $cache->{verbose} );

	# --- VALIDATE ARG LENGTH, EXPORT AND OUT OPTIONS ---
        throw_cmd_validation_exception(
		error => "At least 1-$ARG_MAX variable(s) are required" )
	  unless ( @args && @args <= $ARG_MAX );

        throw_cmd_validation_exception(
		error => "Option 'out' (i.e. output directory) is missing" )
	  unless ( $opts->{out} );

	throw_cmd_validation_exception(
		error => "Make sure '$opts->{out}' exists with RWX enabled (i.e. chmod 777) for CohortExplorer" )
	  unless ( -d $opts->{out} && -w $opts->{out} );

	throw_cmd_validation_exception( error =>
                "Mutually exclusive options (export and export-all) specified together"
	) if ( $opts->{export} && $opts->{export_all} );

	my $datasource = $cache->{datasource};

	if ( $opts->{export} ) {
		my $tables = $datasource->tables();
		my @invalid_tables = grep { !$tables->{$_} } @{ $opts->export };
		throw_cmd_validation_exception( error => "Invalid table(s) "
			  . join( ', ', @invalid_tables )
			  . " in export" )
		  if (@invalid_tables);
	}

	if ( $opts->{export_all} ) {
		$opts->{export} = [ keys %{ $datasource->tables() } ];
	}

	# --- VALIDATE CONDITION OPTION AND ARGS ---

	my @vars = @{ $self->get_validation_variables() };

	for my $var (@args) {
		throw_cmd_validation_exception( error => 
                 "Entity_ID and Visit (if applicable) are already part of the query set"
		) if ( $var =~ /^(Entity_ID|Visit)$/ );

		throw_cmd_validation_exception( error => "Invalid variable '$var' in arguments" )
		  unless ( grep( /^$var$/, @vars ) );
	}

        # Condition can be imposed on all variables including Entity_ID and Visit (if applicable)
	for my $var ( keys %{ $opts->{cond} } ) {

		throw_cmd_validation_exception(
			error => "Invalid variable '$var' in condition option" )
		  unless ( grep( /^$var$/, @vars ) );

		my ( $opr, $val ) =
		  $opts->{cond}{$var} =~
/^\{\'(=|\!=|>|>=|<|<=|between|not_between|like|not_like|in|not_in|regexp)\',(\[(\'[^,\`]+\',?){2,}\]|\'[^\`]+\'|undef)\}$/;

		# Validating SQL conditions
                if ( $opr && $val ) {

                        # Operators (between, not_between, in and not_in) require array but for others it is optional
			if ( $opr =~ /(between|in)/ ) {
				throw_cmd_validation_exception(
					error => "Expecting array for '$opr' in '$val'" )
				  if ( ref eval $val ne 'ARRAY' );
				throw_cmd_validation_exception(
					error => "Expecting min and max for '$opr' in '$val'" )
				  if ( $1 eq 'between' && scalar @{ eval $val } != 2 );
			}
		}
		else {
			throw_cmd_validation_exception(
				error => "Invalid format for condition option ('$var')" );
		}
	}
}

sub run {

	# Overall running of the command
	my ( $self, $opts, @args ) = @_;
	my $cache = $self->cache->get('cache');
	my $result_set = $self->process( $opts, $cache, @args );

	$self->save_command( $opts, $cache, @args ) if ( $opts->{save_command} );

	# If the result-set is not empty
	if (@$result_set) {
		my $dir = $1 if ( $opts->{out} =~ /^(.+)$/ );

		require Text::CSV_XS;

		# Initialise the csv object
		my $csv = Text::CSV_XS->new(
			{
				'quote_char'  => '"',
				'escape_char' => '"',
				'sep_char'    => ',',
				'binary'      => 1,
				'auto_diag'   => 1,
                                'eol'         => $/
			}
		);
		$self->export_data( $opts, $cache, $result_set, $dir, $csv, @args );

		return {
			headingText => 'summary statistics',
			rows => $self->summary_stats( $opts, $cache, $result_set, $dir, $csv )
		  }
		  if ( $opts->{stats} );
	}

	return undef;
}

sub process {

	my ( $self, $opts, $cache, @args ) = @_;

	my $datasource = $cache->{datasource};
	my $sqla       = $datasource->sqla();
 
        # --- PREPARE QUERY PARAMETERS FROM CONDITION OPTION AND ARGS ---

        # Query parameters can be static, dynamic or both
        # Static type is applicable to 'standard' datasource but it may also be applicable
        # to 'longitudinal' datasource provided the datasource contains tables which
        # are independent of visits (i.e. static tables). Dynamic type is associated
        # with longitudinal datasources only
        my $param = $self->get_query_parameters( $opts, $datasource, @args );

	my ( $stmt, $var, $sth, @rows );

	( my $command = lc ref $self ) =~ s/^.+:://;

	for my $type ( keys %$param ) {
		eval {
			( $param->{$type}{stmt}, @{ $param->{$type}{bind} } ) =
			  $sqla->select( %{ $param->{$type} } );
		};

		if ( catch my $e ) {
			throw_cmd_run_exception( error => $e );
		}

		# Check if variable(s) are specified within condition(s)
		# If yes, then remove variables from @bind as they need to be hard coded

		require Tie::IxHash;    # May or may not be preloaded

		tie my %vars, 'Tie::IxHash', map { $_ => 1 } ( '`Entity_ID`', $param->{$type}{stmt} =~ /AS\s+(\`[^\`]+\`),?/g );

                my @quoted_bind = map { s/\'//g; "`$_`" } @{$param->{$type}{bind} };
                my @var_placeholder = grep ( defined $vars{$quoted_bind[$_]}, 0 .. $#quoted_bind );
                
                if (@var_placeholder) {
                     # No variables in placeholders as they need to be hard coded
		     for ( 0 .. $#var_placeholder ) {
				my $count = 0;
                                $param->{$type}{stmt} =~ s/(\?)/$count++ == $var_placeholder[$_]-$_ ? $quoted_bind[$var_placeholder[$_]] : $1/ge;
				delete( $param->{$type}{bind}->[ $var_placeholder[$_] ] );
		      }
		      @{ $param->{$type}{bind} } = grep( defined($_), @{ $param->{$type}{bind} } );
		}

		delete $vars{'`Entity_ID`'};
		$var->{$type} = [ keys %vars ];
	}

	if ( keys %$param == 1 ) {    # either static or dynamic parameter
		$stmt = $param->{ ( keys %$param )[0] }{stmt};
	}

	else {  # both static and dynamic parameters are present

                # Give priority to visit dependent tables (i.e. dynamic tables) therefore do left join
                # Inner join is done when conditions are imposed on static tables alone
                $stmt =
		    'SELECT dynamic.Entity_ID, '
		  . join( ', ', map { @{ $var->{$_} } } keys %$var )
		  . ' FROM '
		  . join(
			  ( ( ( !$param->{static}{-having}{Entity_ID} && keys % { $param->{static}{-having} } == 1) 
                               || keys % { $param->{static}{-having} } > 1 
                             )  ? ' INNER JOIN ' : ' LEFT OUTER JOIN '),
			map { "( " . $param->{$_}{stmt} . " ) AS $_" } keys %$param
		  ) . ' ON dynamic.Entity_ID = static.Entity_ID';
	}

	my @bind = map { @{ $param->{$_}{bind} } } keys %$param;

	print STDERR "Running the query with "
	  . scalar @bind
	  . " bind variables ..." . "\n\n"
	  if ( $cache->{verbose} );

	require Time::HiRes;

	my $timeStart = Time::HiRes::time();

	eval {
		$sth = $datasource->dbh()->prepare_cached($stmt);
		$sth->execute(@bind);
	};

	if ( catch my $e ) {
		throw_cmd_run_exception( error => $e );
	}

	my $timeEnd = Time::HiRes::time();

	printf( "Found %d rows in %.2f sec matching the %s query criteria ...\n\n",
		($sth->rows() || 0),
		($timeEnd - $timeStart),
                 $command
	) if ( $cache->{verbose} );

	push @rows, ( $sth->{NAME}, @{ $sth->fetchall_arrayref( [] ) } )
	  if ( $sth->rows() );
	$sth->finish();
	return \@rows;
}

sub save_command {

	my ( $self, $opts, $cache, @args ) = @_;
	my $alias = $cache->{datasource}->alias();
	my $count = scalar keys %{ $COMMAND_HISTORY->{datasource}{$alias} };
      ( my $command = lc ref $self ) =~ s/^.+:://;

	print STDERR "Saving command ..." . "\n\n" if ( $cache->{verbose} );

	require POSIX;

        # Remove the save-command option
        delete $opts->{save_command};

	# Construct the command run by the user and store it in $COMMAND_HISTORY
	for my $opt ( keys %$opts ) {
		if ( ref $opts->{$opt} eq 'ARRAY' ) {
			 $command .= " --$opt=" . join( " --$opt=", @{ $opts->{$opt} } );
		}
		elsif ( ref $opts->{$opt} eq 'HASH' ) {
			$command .= join(
				' ',
				map ( "--$opt=$_=\"$opts->{$opt}{$_}\" ",
					keys %{ $opts->{$opt} } )
			);
		}
		else {
			( $_ = $opt ) =~ s/_/-/g;
			$command .= " --$_=$opts->{$opt} ";
			$command =~ s/($_)=1/$1/ if ( $opts->{export_all} || $opts->{stats} );
		}
	}

	$command .= ' ' . join( ' ', @args );
	$command =~ s/\-\-export=[^\s]+\s*/ /g if ( $opts->{export_all} );
	$command =~ s/\s+/ /g;

	for ( keys %{ $COMMAND_HISTORY->{datasource} } ) {
		  $COMMAND_HISTORY->{datasource}{$_}{ ++$count } = {
			datetime => POSIX::strftime( '%d/%m/%Y %T', localtime ),
			command  => $command
		  }
		  if ( $_ eq $alias );

	}
}

sub export_data {

	my ( $self, $opts, $cache, $result_set, $dir, $csv, @args ) = @_;

	my $datasource = $cache->{datasource};

	# Write query param file
        my $file = File::Spec->catfile($dir, "QueryParameters");
	my $fh = FileHandle->new("> $file")	  
           or throw_cmd_run_exception( error => "Failed to open file: $!" );

	print $fh "Query Parameters" . "\n\n";
	print $fh "Arguments supplied: " . join( ', ', @args ) . "\n\n";
	print $fh "Conditions imposed: "
	  . scalar( keys %{ $opts->{cond} } ) . "\n\n";

	my @condition = keys %{ $opts->{cond} };

	for ( 0 .. $#condition ) {
		$opts->{cond}{ $condition[$_] } =~ /^\{\'([^\']+)\',(.+)\}$/o;
		print $fh ( $_ + 1 ) . ") $condition[$_]: '$1' => $2" . "\n";
	}
	print $fh "\n"
	  . "Tables exported: "
	  . ( $opts->{export} ? join ', ', @{ $opts->{export} } : 'None' ). "\n";

	$fh->close();

	print STDERR "Exporting query results in $dir ..." . "\n\n"
	  if ( $cache->{verbose} );

	my $result_entity =
	  $self->process_result_set( $opts, $datasource, $result_set, $dir, $csv,
		@args );

	if ( $opts->{export} ) {
		my $tables = $datasource->tables();
		my $sqla   = $datasource->sqla();
		my $dbh    = $datasource->dbh();
		my ( $stmt, @bind, $sth );
		my $struct = $datasource->entity_structure();
		eval {
			( $stmt, @bind ) = $sqla->select(
				-columns => [
					map { $struct->{-columns}{$_} || 'NULL' }
					  qw/entity_id variable value visit/
				],
				-from  => $struct->{-from},
				-where => {
					%{ $struct->{-where} },
					$struct->{-columns}{table} => { '=' => '?' }
				}
			);
		};

		if ( catch my $e ) {
			throw_cmd_run_exception( error => $e );
		}

		$sth = $dbh->prepare_cached($stmt);

		# Get the index of 'table' placeholder in @bind
		my @chunk = split /\?/, $stmt;
		my ($placeholder) =
		  grep { $chunk[$_] =~ /\s+$struct->{-columns}{table}\s+=\s+/ }
		  0 .. $#chunk;

		for my $table ( @{ $opts->{export} } ) {
                        # Ensure the user has access to at least one variable in the table to be exported ...
			if (
				grep ( /^$table\..+$/,
					keys %{ $cache->{datasource}->variables() } ) )
			{
				$bind[$placeholder] = $table;
				eval { $sth->execute(@bind); };

				if ( catch my $e ) {
					throw_cmd_run_exception( error => $e );
				}

				my $table_data = $sth->fetchall_arrayref( [] );
				$sth->finish();

				if (@$table_data) {
					print STDERR "Exporting $table ..." . "\n\n"
					  if ( $cache->{verbose} );
					$self->process_table( $table, $datasource, $table_data,
						$dir, $csv, $result_entity );
				}
				else {
					print STDERR "Omitting $table (no entities) ..." . "\n\n"
					  if ( $cache->{verbose} );
				}

			}
			else {
				print STDERR "Omitting $table (no variables) ..." . "\n\n"
				  if ( $cache->{verbose} );
			}
		}

	}
}

sub summary_stats {

	my ( $self, $opts, $cache, $result_set, $dir, $csv ) = @_;

	print STDERR "Preparing dataset for computing summary statistics ..."
	  . "\n\n"
	  if ( $cache->{verbose} );

	# Prepare data for computing summary statistics from the result set

	my ( $data, $key_index, @cols ) = $self->get_stats_data($result_set);

	my $vars = $cache->{datasource}->variables();

        my $file = File::Spec->catfile($dir, "SummaryStatistics.csv");
	my $fh = FileHandle->new("> $file")
	  or throw_cmd_run_exception( error => "Failed to open file: $!" );

	$csv->print($fh, \@cols) or throw_cmd_run_exception( error => $csv->error_diag() );

	push my @summary_stats, [@cols];

	my @keys = $cols[0] eq 'Visit' ? sort { $a <=> $b } keys %$data : sort keys %$data;

	@cols = $key_index == 0 ? @cols : splice @cols, 1;

	print STDERR "Computing summary statistics for "
	  . ( $#cols + 1 )
	  . " query variable(s): "
	  . join( ', ', @cols ) . " ... \n\n"
	  if ( $cache->{verbose} );

      # Key can be Visit, Entity_ID or none depending on the command (i.e. search/compare) run.
      # For longitudinal datasources the search command computes statistics with respect to visit,
      # hence the key is 'Visit'. Standard datasources are not visit based so no key is used.
      # Compare command uses Entity_ID as the key when computing statistics for longitudinal datasources.
      require Statistics::Descriptive;

      for my $key (@keys) {
		push my @row, ( $key_index == 0 ? () : $key );
		for my $col (@cols) {
			my $sdf = Statistics::Descriptive::Full->new();

			# Computing statistics for integer/decimal variables
			if (   $vars->{$col}
				&& ( $vars->{$col}{type} =~ /(signed|decimal)/i )
				&& @{ $data->{$key}{$col} } )
			{

				# Remove single/double quotes (if any) from the numeric array
				$sdf->add_data( map { s/[\'\"]+//; $_ } @{ $data->{$key}{$col} } );

				eval {
					push @row,
					  sprintf(
                                                "N: %3s\nMean: %.2f\nMedian: %.2f\nSD: %.2f\nMax: %.2f\nMin: %.2f",
						$sdf->count(), $sdf->mean(), $sdf->median(),
						$sdf->standard_deviation(),
						$sdf->max(), $sdf->min()
					  );
				};

				if ( catch my $e ) {
					throw_cmd_run_exception($e);
				}
			}

                       # Computing statistics for categorical variables with type 'text' and bolean variables only
			elsif ($vars->{$col}
				&& $vars->{$col}{type} =~ /^char/i
				&& $vars->{$col}{category} )
			{
				my $N = @{ $data->{$key}{$col} } || 1;

				tie my %category, 'Tie::IxHash',
				  map { /^([^,]+),\s*(.+)$/, $1 => $2 } split /\s*\n\s*/,
				  $vars->{$col}{category};

				# Order of categories should remain the same
				tie my %count, 'Tie::IxHash', map { $_ => 0 } keys %category;

				# Get break-down by each category
				for ( @{ $data->{$key}{$col} } ) {
					$count{$_}++;
				}

				push @row,
				  sprintf( "N: %1s\n", scalar @{ $data->{$key}{$col} } )
				  . join "\n", map {
					sprintf( ( $category{$_} || $_ ) . "\: %1.2f%-8s",
						$count{$_} * 100 / $N, '%' )
				  } keys %count;
			}

                        # For all other variable types (e.g. date, datetime etc.) get no. of observations alone
			else {
				push @row,
				  sprintf( "N: %3s\n", scalar @{ $data->{$key}{$col} } );
			}
		}

		$csv->print($fh, \@row) or throw_cmd_run_exception( error => $csv->error_diag() );
		push @summary_stats, [@row];
	}

	$fh->close();

	return \@summary_stats;

}

#------------- SUBCLASSES HOOKS -------------#

sub usage_text { }

sub get_validation_variables { }

sub get_query_parameters { }

sub process_result_set { }

sub process_table { }

sub get_stats_data { }

END {
        # Write saved commands to command history file
	eval {
		$COMMAND_HISTORY_CONFIG->save_file( $COMMAND_HISTORY_FILE, $COMMAND_HISTORY );
	};

	if ( catch my $e ) {
		throw_cmd_run_exception( error => $e );
	}

}

#-------
1;

__END__

=pod

=head1 NAME

CohortExplorer::Command::Query - CohortExplorer abstract class to search and compare command classes

=head1 DESCRIPTION

This class serves as the base class to search and compare command classes. The class is inherited from L<CLI::Framework::Command> and overrides the following methods:

=head2 option_spec()

Returns application option specifications as expected by L<Getopt::Long::Descriptive>

       ( 
         [ 'cond|c=s%'      => 'impose conditions'                           ],
         [ 'out|o=s'        => 'provide output directory', { required => 1 } ],
         [ 'save-command|s' => 'save command'                                ],
         [ 'stats|S'        => 'show summary statistics'                     ],
         [ 'export|e=s@'    => 'export tables by name'                       ],
         [ 'export-all|a'   => 'export all tables'                           ] 
       )

=head2 validate( $opts, @args )

This method validates the command options and arguments and throws exceptions when validation fails.

=head2 run( $opts, @args )

This method is responsible for the overall functioning of the command. The method calls option specific methods for option specific processing.


=head1 OPTION SPECIFIC PROCESSING

=head2 get_validation_variables()

This method should return a ref to the list for validating variables present under arguments and condition option(s).

=head2 process( $opts, $cache, @args )

The method attempts to query the database using the SQL constructed from the hash ref, returned from L<get_query_parameters|/get_query_parameters( $opts, $datasource, @args )>. Upon successful execution of the SQL query the method returns the output (i.e. C<$result_set>) which is a ref to array of arrays where each array corresponds to one row of entity data.

=head2 save_command( $opts, $cache, @args)

This method is only called if the user has specified the save command option (i.e. C<--save-command>). The method first constructs the command from command options and arguments (i.e. C<$opts> and C<@args>) and adds it to the C<$COMMAND_HISTORY> hash along with the datetime information. The C<$COMMAND_HISTORY> contains all commands previously saved by the user. 


=head2 export_data( $opts, $cache, $result_set, $dir, $csv, @args )

This method creates a output directory under the directory specified by the C<--out> option and calls L<process_result_set|/process_result_set( $opts, $datasource, $result_set, $dir, $csv, @args )> method of the subclass. The further processing by the method depends on the presence of C<--export> option(s). If the user has provided the C<--export> option, the method first constructs the SQL from L<entity_structure|CohortExplorer::Datasource/entity_structure()> with a table name placeholder. The method executes the same SQL with a different bind parameter (i.e. table name) depending upon the number of tables to be exported. The output obtained from successful execution of SQL is passed to L<process_table|/process_table( $table, $datasource, $table_data, $dir, $csv, $result_entity )> for further processing.


=head2 summary_stats( $opts, $cache, $result_set, $dir, $csv )

This method is only called if the user has specified summary statistics (i.e. C<--stats>) option. The method computes the descriptive statistics from the data frame returned by L<get_stats_data|/get_stats_data( $result_set )>.


=head1 SUBCLASS HOOKS

=head2 usage_text()

This method should return the usage information for the command.

=head2 get_query_parameters( $opts, $datasource, @args )

This method should return a hash ref with keys, C<static>, C<dynamic>, or C<both> depending on the datasource type and variables supplied in arguments and conditions. As a standard datasource has all static tables so the hash ref must contain only one key, C<static> where as a longitudinal datasource may contain both keys, C<static> and C<dynamic> provided the datasource has static tables. The parameters to the method are as follows:

C<$opts> an options hash with the received command options as keys and their values as hash values.

C<$datasource> is the datasource object. 

C<@args> arguments to the command.

=head2 process_result_set( $opts, $datasource, $result_set, $dir, $csv, @args )

This method should process the result set obtained after running the SQL query and write a csv file. If the variables provided as part of arguments and conditions belong only to the static tables, the method should return a ref to list of entities present in the result set. Otherwise, the method should return a hash ref with C<Entity_ID> as keys and corresponding visit numbers as values. 

In this method, 

C<$opts> an options hash with the received command options as keys and their values as hash values.

C<$datasource> is the datasource object.

C<$result_set> is the output obtained upon SQL execution. 

C<$dir> is the export directory.

C<$csv> is the object of L<Text::CSV_XS>.

C<@args> arguments to the command.

=head2 process_table( $table, $datasource, $table_data, $dir, $csv, $result_entity )

This method should process the table data obtained from running the export SQL query. The method should write the table data in a csv file for all entities present in the result set.

The parameters to the method are:

C<$table> is the name of the table to be exported.

C<$datasource> is the datasource object.

C<$table_data> is the output obtained from executing the export SQL query.

C<$dir> is the export directory.

C<$csv> is the object of L<Text::CSV_XS>.

C<$result_entity> is a ref to all entities present in the result-set. If variables present in C<cond> option and C<@args> belong to static tables the ref is simply to the list containing C<Entity_IDs>. Otherwise, the reference is to a hash where C<Entity_IDs> are keys and corresponding visit numbers are values.

=head2 get_stats_data( $result_set )

This method should generate the data for computing summary statistics. The method should return a hash ref with key as the parameter, the statistics are computed with respect to and values as the hash with variable names as keys and array ref as hash values.

=head1 DIAGNOSTICS

CohortExplorer::Command::Query throws following exceptions imported from L<CLI::Framework::Exceptions>:

=over

=item 1

C<throw_cmd_run_exception>: This exception is thrown if one of the following conditions are met:

=over

=item *

The command history file fails to load. For the save command option to work it is required that the file C<$HOME/.CohortExplorer_History> exists with RWX enabled for CohortExplorer.

=item *

The C<select> method from L<SQL::Abstract::More> fails to construct the SQL from the supplied hash ref.

=item *

The method C<execute> from L<DBI> fails to execute the SQL query.

=item *

The full methods under package L<Statistics::Descriptive> fail to compute statistics.

=back

=item 2

C<throw_cmd_validation_exception>: This exception is thrown whenever the command options/arguments fail to validate.

=back

=head1 DEPENDENCIES

L<CLI::Framework::Command>

L<CLI::Framework::Exceptions>

L<Config::General>

L<DBI>

L<Exception::Class::TryCatch>

L<FileHandle>

L<File::HomeDir>

L<File::Spec>

L<SQL::Abstract::More>

L<Statistics::Descriptive>

L<Text::CSV_XS>

L<Tie::IxHash>

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
