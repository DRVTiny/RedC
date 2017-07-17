#!/usr/bin/perl
package RedC;
use parent 'Redis::Fast';
use strict;
use utf8;
binmode $_=>':utf8' for *STDOUT,*STDERR;
use 5.16.1;
use constant {
    RECONNECT_UP_TO=>10,	# Reconnect up to 10 seconds
    RECONNECT_EVERY=>100,	# Reconnect every 100 ms
};

use CBOR::XS;
use Carp qw(confess);
use Scalar::Util qw(refaddr);

use Exporter qw(import);
our @EXPORT_OK=qw(get_redc_by);

my (%conoByName, %conoByRef);
# By default, after connection failure try to reconnect every 100 ms up to 10 seconds
my $redTestObj=Redis::Fast->new('reconnect'=>RECONNECT_UP_TO,'every'=>RECONNECT_EVERY);
my $cbor=CBOR::XS->new;

sub new {
    my ($class, %options)=@_;
    
    my ($coName,$conIndex)=delete @options{qw/name index/};
    $conIndex=0 unless defined $conIndex and $conIndex!~/[^\d]/;
    
    confess qq(Cant initialize RedC: name '$coName' was already reserved as Redis connector name)
        if $conoByName{$coName};
    
    if ($conIndex) {
        my $nDB=databases($redTestObj);
        confess sprintf('Redis database index #%s is out of configured bounds (min=0, max=%d)',$conIndex,$nDB-1)
            if $conIndex>=$nDB;
    }
    
    my $redC=$class->SUPER::new(
# Hint: you can redefine "reconnection" settings in your %options passed to constructor
        'reconnect'=>RECONNECT_UP_TO,
        'every'=>RECONNECT_EVERY,
        $conIndex
            ? ('on_connect'=>sub {
                    $_[0]->SUPER::select($conIndex);
              })
            : (),
        %options,
    );
    
    $conoByRef{refaddr $redC}=$conoByName{$coName}={'name'=>$coName,'index'=>$conIndex,'redc'=>\$redC};
    return $redC;
}

sub select {
    confess 'Redis method "select" is prohibited for '.__PACKAGE__.' objects'
}

my $CBOR_MAGIC=$CBOR::XS::MAGIC;

sub write {
    my $redC=shift;
    my $cb=pop(@_) if ref $_[$#_] eq 'CODE';
    return unless @_;
    my $method=(@_>2?'m':'').'set';
    $redC->$method(
        (map { 
            my ($k,$v)=(shift,shift); 
            $k=>ref($v)
                ? $CBOR_MAGIC.$cbor->encode($v)
                : do { utf8::encode($v) if utf8::is_utf8($v); $v }
        } 1..+(scalar(@_)>>1)),
        $cb?($cb):()
    );
}

my $readCallback=<<'EOSUB';
    sub {
        if ( defined $_[1] ) {
            if ($cb) {
                $cb->(@_)
            } else {
                confess 'Redis error: '.$_[1]
            }
        }
        my $cbor=CBOR::XS->new;
        my $CBOR_MAGIC=$CBOR::XS::MAGIC;
        $retv=
            $opts->{'ret_hash_ref'}
            ? do {
                my $c=0;
                scalar({ map { $k->[$c++]=>%ALTCODE% } ref($_[0]) eq 'ARRAY'?@{$_[0]}:($_[0]) })
              }
            : [ map { %ALTCODE% } ref($_[0]) eq 'ARRAY'?@{$_[0]}:($_[0]) ];
        $cb->( $retv ) if $cb;
    }
EOSUB
my @altReadCode=map {$readCallback=~s/%ALTCODE%/$_/gre} (
    <<'EOCODE',
        ( defined($_) and length($_)>3 and substr($_,0,3) eq $CBOR_MAGIC )
            ? $cbor->decode($_)
            : defined($_)
                ? do { utf8::decode($_); $_ }
                : undef
EOCODE
        q{defined($_)?$cbor->decode($_):undef},
);

sub read {
    my $redC=shift;
    my $opts=ref($_[0]) eq 'HASH'?do { $_=shift; %{$_}?$_:{'ret_hash_ref'=>1} }:undef;
    my $cb=pop(@_) if ref $_[$#_] eq 'CODE';
    return unless @_;
    my $k=[map {ref($_) eq 'ARRAY'?@{$_}:$_} @_];
    my $method=($#{$k}?'m':'').'get';
    my $retv;
    $redC->$method(@{$k}, eval($altReadCode[$opts->{'only_cbor'}?1:0]) || confess 'EVAL ERROR: '.$@);
    return 1 if $cb;
    $redC->wait_all_responses;
    return $retv;
}

sub databases {
    my $redC=shift;
    open my $fh, '<', $redC->info->{'config_file'};
    return ((local $/=<$fh>)=~m/(?:^|\n)\s*databases\s+(\d+)\s*(?:$|\n)/sm)[0];
}

sub get_redc_by {
    shift if ref $_[0] eq __PACKAGE__;
    my $byWhat=shift;
    return if ref $byWhat or !defined($byWhat);
    given ($byWhat) {
        return $conoByName{$byWhat} 
            when 'name';
        return [grep {$_->{'index'}==$byWhat} values %conoByName] 
            when 'index';
        default {
            confess 'No such connector object attribute: '.$byWhat
        }
    }
}

sub DESTROY {
    delete $conoByName{delete($conoByRef{refaddr $_[0]})->{'name'}};
}

1;
