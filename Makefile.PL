use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
	NAME         => 'CohortExplorer',
	AUTHOR       => 'Abhishek Dixit <adixit@cpan.org>',
	VERSION_FROM => 'lib/CohortExplorer.pm',
	ABSTRACT => 'Explore clinical cohorts and search for entities of interest',
	( $ExtUtils::MakeMaker::VERSION >= 6.3002
		? ( 'LICENSE' => 'perl' )
		: () ),
	PL_FILES  => {},
	PREREQ_PM => {
		'Test::More'                 => 0,
		'CLI::Framework'             => 0,
		'Config::General'            => 0,
		'DBI'                        => 0,
		'Exception::Class::TryCatch' => 0,
		'FileHandle'                 => 0,
		'Log::Log4perl'              => 0,
		'LWP::UserAgent'             => 0,
		'MIME::Base64'               => 0,
		'POSIX'                      => 0,
		'SQL::Abstract::More'        => 0,
		'Statistics::Descriptive'    => 0,
		'Text::CSV_XS'               => 0,
		'Text::ASCIITable'           => 0,
		'Time::HiRes'                => 0,
		'Tie::IxHash'                => 0,
		'Term::ReadKey'              => 0,
	},
	dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
	clean => { FILES    => 'CohortExplorer-*' },
);
