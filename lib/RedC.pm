#!/usr/bin/perl
package RedC;
use constant {
    RECONNECT_UP_TO	=>	 10,		# Reconnect up to 10 seconds
    RECONNECT_EVERY	=>	 100_000,	# Reconnect every 100 ms
    DFLT_ENCODER_TAG	=> 	'CB',		# Ecnode complex structures to CBOR by default
};

use strict;
use parent 'Redis::Fast';
use utf8;
binmode $_=>':utf8' for *STDOUT,*STDERR;
use 5.16.1;

use Tag::DeCoder;
use Carp qw(confess);
use Ref::Util qw(is_hashref is_plain_coderef is_plain_hashref is_plain_arrayref);
use Scalar::Util qw(refaddr);

use Exporter qw(import);
our @EXPORT_OK=qw(get_redc_by);

my (%conoByName, %conoByRef);
# By default, after connection failure try to reconnect every 100 ms up to 10 seconds
my $redTestObj = Redis::Fast->new('reconnect'=>RECONNECT_UP_TO, 'every'=>RECONNECT_EVERY);
sub new {
    state $exclOptions={'on_connect'=>1, 'encoder'=>1};
    my ($class, %options)=@_;
    
    my ($coName, $conIndex) = delete @options{qw/name index/};
    $conIndex=0 unless defined $conIndex and $conIndex!~/[^\d]/;
    $coName //= join('_' => 'redc', 'anon', 'pid'.$$, int(rand 65536));
    confess qq(Cant initialize RedC: name '$coName' was already reserved as Redis connector name)
        if $conoByName{$coName};
    
    if ($conIndex) {
        my $nDB = databases($redTestObj);
        confess sprintf('Redis database index #%s is out of configured bounds (min=0, max=%d)',$conIndex, $nDB-1)
            if $conIndex>=$nDB;
    }
    
    my $redC = $class->SUPER::new(
# Hint: you can redefine "reconnection" settings in your %options passed to constructor
        'reconnect'	=>	RECONNECT_UP_TO,
        'every'		=>	RECONNECT_EVERY,
        'on_connect'	=>	sub {
            my $self = shift;
            $options{'on_connect'} and is_plain_coderef($options{'on_connect'}) and $options{'on_connect'}->($self);
            $self->SUPER::select($conIndex) if $conIndex;
            $self->SUPER::client_setname(
                (
                    (defined($options{'client_name'}) and !ref $options{'client_name'})
                        ? $options{'client_name'}
                        : "${coName}_$$"
                ) =~ s%\s%_%gr
            );
        },
        (map {$_=>$options{$_}} grep { !$exclOptions->{$_} } keys %options),
    );
    $conoByRef{refaddr $redC} = $conoByName{$coName} = {'name'=>$coName, 'index'=>$conIndex, 'redc'=>\$redC};
    return $redC;
}

sub encoder {
    $_[1] ? $conoByRef{refaddr $_[0]}{'encoder_tag'} = $_[1] : $conoByRef{refaddr $_[0]}{'encoder_tag'} //= DFLT_ENCODER_TAG
}

sub select {
    confess 'Redis method "select" is prohibited for '.__PACKAGE__.' objects'
}

sub write {
    my $redC = shift;
    
    my $cb = pop(@_) if is_plain_coderef($_[$#_]);
    return unless @_;
    my $method = (@_>2 ? 'm' : '') . 'set';
    my $encTag = $redC->encoder;
    $redC->$method(
        ( map {
            my ($k, $v) = (shift, shift);
            $k => ref($v)
                ? encodeByTag($encTag => $v)
                : do { utf8::encode($v) if utf8::is_utf8($v); $v }
        } 1..+(scalar(@_)>>1) ),
        $cb ? ($cb) : ()
    );
}

sub read {
    my $redC = shift;
    my $opts = is_plain_hashref($_[0]) ? do { $_=shift; %{$_}? $_ : {'ret_hash_ref'=>1} } : undef;
    my $cb = pop(@_) if is_plain_coderef($_[$#_]);
    return unless @_;
    my $flRetHashRef = $opts->{'ret_hash_ref'};
    my $k = [ map { is_plain_arrayref($_) ? @{$_} : $_ } @_ ];
    my $method = ($#{$k} ? 'm' : '') . 'get';
    my $retv;
    $redC->$method( @{$k} => 
    sub {
        if ( defined $_[1] ) {
            if ( $cb ) {
                $cb->(@_)
            } else {
                confess 'Redis error: '.$_[1]
            }
        }
        $retv=
            $flRetHashRef
            ? do {
                my $c=0;
                scalar({ map { $k->[$c++] => decodeByTag($_) } is_plain_arrayref($_[0]) ? @{$_[0]} : ($_[0]) })
              }
            : [ map decodeByTag($_), is_plain_arrayref($_[0]) ? @{$_[0]} : ($_[0]) ];
        $cb->( $retv ) if $cb;
    });
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
    delete $conoByName{ delete($conoByRef{refaddr $_[0]})->{'name'} };
}

1;
