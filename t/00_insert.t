#!/usr/bin/perl

use strict;
use lib qw(../lib);
use Sql::Simple;
use Test;

$Sql::Simple::RETURNSQL = 1;

BEGIN { plan tests => 5 }

my $valid = [
  "INSERT INTO randomtable\n( column1, column2 )\nVALUES\n( ?, ? )",
  "INSERT INTO randomtable\n( column1, column2 )\nVALUES\n( ?, ? )",
"INSERT INTO randomtable
( column1, column2 )
( SELECT othertable.value1, othertable.value2 FROM othertable WHERE randomcolumn = ? )",
  "INSERT INTO randomtable\n( column1, column2 )\nVALUES\n( ?, ? )",
  "INSERT INTO randomtable\n( column1, column2 )\nVALUES\n( ?, ? )",
];

my $test_list = [
  [ 'randomtable', [ qw(column1 column2) ], [ qw(value1 value2) ] ],
  [ 
    'randomtable', 
    [ qw(column1 column2) ], 
    [
      [ qw(value1 value2) ],
      [ qw(value3 value4) ]
    ],
  ],
  [ 
    'randomtable', 
    [ qw(column1 column2) ], 
    { 
      'columns' => [ qw(value1 value2) ], 
      'table'   => 'othertable',
      'where'   => {
	'randomcolumn' => 'randomvalue'
      }
    }
  ],
  [
    'randomtable',
    {
      'column1' => 'value',
      'column2' => 'value2'
    }
  ],
  [
    'randomtable',
    [
      {
        'column1' => 'value',
        'column2' => 'value2'
      },
      {
	'column1' => 'value3',
	'column2' => 'vlaue4'
      }
    ]
  ]
];
 
foreach my $c ( 0..$#{$valid} ) {
  eval {
    if ( Sql::Simple->insert(@{$test_list->[$c]}, 1) eq $valid->[$c] ) {
      ok(1);
    } else {
      ok(0);
    }
  };
  ok(0) if ( $@ );
  warn $@ if ( $@ );;
}
