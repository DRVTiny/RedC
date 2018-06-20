#!/usr/bin/perl
package RedC;
use constant {
    RECONNECT_UP_TO	=>	 10,		# Reconnect up to 10 seconds
    RECONNECT_EVERY	=>	 100_000,	# Reconnect every 100 ms
    DFLT_ENCODER_TAG	=> 	'CB',		# Ecnode complex structures to CBOR by default
    DFLT_REDIS_DB_N	=>	 0,
};

use strict;
use parent 'Redis::Fast';
use utf8;
binmode $_=>':utf8' for *STDOUT,*STDERR;
use 5.16.1;
use Data::Dumper;
use Tag::DeCoder;
use Carp		qw(confess);
use Ref::Util 		qw(is_hashref is_plain_coderef is_plain_hashref is_plain_arrayref);
use Scalar::Util 	qw(refaddr);
use Exporter qw(import);
our @EXPORT_OK = qw(get_redc_by);

my (%conoByName, %conoByRef);

sub dump_cono_by {
    say Dumper +{
        'by_name' => \%conoByName,
        'by_ref'  => \%conoByRef,
    }
}
BEGIN {
    my %methodsGen = (
        'write' => {
            'variants' => {
                'fast' => {
                    'check_for_null' => '$v //= "";'
                },
                'safe' => {
                    'suffix' => '_not_null',
                    'check_for_null' => 'defined($v) or confess qq(illegal attempt to write undef value for <<$k>> key);'
                },
            }
        },
        'read' => {
            'variants' => {
                'null_is_undef' => {
                    'if_null_hsh' => '($key => undef)',
                    'if_null_arr' => 'undef',
                },
                'null_is_prohibited' => {
                    'suffix' => '_not_null',
                    'if_null_hsh' => 'confess sprintf(q{illegal attempt to read value of unexisting key <<%s>> in redis_db #%d named here as <<%s>>}, $key, $redC->index, $redC->name)',
                    'if_null_arr' => 'confess sprintf(q{illegal attempt to read value of unexisting key <<%s>> in redis_db #%d named here as <<%s>>}, $key, $redC->index, $redC->name)',
                },
            }
        },
    );
    
    $methodsGen{'write'}{'template'} = <<'EOMETHOD';
        state $errEmptyList = 'Cant write empty list anywhere';
        my $redC = shift;
        @_ or confess $errEmptyList;
        my $cb = pop(@_) if is_plain_coderef($_[$#_]);
        return unless @_;
        my $encTag = $redC->encoder;
        if ( is_plain_hashref($_[0]) ) {
            %{$_[0]} or confess $errEmptyList; # there is nothing to write
            my $data = $_[0];
            my $method = (keys($data) > 1 ? 'm' : '') . 'set';
            $redC->$method(
                ( map {
                    my ($k, $v) = each $data;
                    <<CHECK_FOR_NULL>>
                    $k => ref($v)
                        ? encodeByTag($encTag => $v)
                        : do { utf8::is_utf8($v) and utf8::encode($v); $v }
                } 1..keys($data) ),
                $cb ? ($cb) : ()
            )
        } else {
            my $method = (@_ > 2 ? 'm' : '') . 'set';
            $redC->$method(
                ( map {
                    my ($k, $v) = (shift, shift);
                    <<CHECK_FOR_NULL>>
                    $k => ref($v)
                        ? encodeByTag($encTag => $v)
                        : do { utf8::is_utf8($v) and utf8::encode($v); $v }
                } 1..+(scalar(@_) >> 1) ),
                $cb ? ($cb) : ()
            )
        }
EOMETHOD

    $methodsGen{'read'}{'template'} = <<'EOMETHOD';
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
            $retv = 
                $flRetHashRef
                ? do {
                    my $c = 0;
                    +{ 
                        map {
                            my $key = $k->[$c++];
                            defined($_)
                                ? ($key => decodeByTag($_))
                                : <<IF_NULL_HSH>>
                        } is_plain_arrayref($_[0]) ? @{$_[0]} : ($_[0])
                    }
                  }
                : do {
                    my $c = 0;
                    [ 
                        map { 
                            my $key = $k->[$c++];
                            defined($_)
                                ? decodeByTag($_)
                                : <<IF_NULL_ARR>>
                        } is_plain_arrayref($_[0]) ? @{$_[0]} : ($_[0]) 
                    ]
                  };
            $cb->( $retv ) if $cb;
        });
        return 1 if $cb;
        $redC->wait_all_responses;
        return $retv;
EOMETHOD
    
    while (my ($methodCmnName, $methodGen) = each %methodsGen) {
        no strict 'refs';
        my $methodTmplCode = sprintf("sub {\n%s\n}", $methodGen->{'template'});
        for my $methodCodePatch ( values $methodGen->{'variants'} ) {
            my $methodGenName = $methodCmnName . ($methodCodePatch->{'suffix'} // '');
            *{__PACKAGE__ . '::' . $methodGenName} = eval($methodTmplCode =~ s%<<([A-Z][A-Z_]+[A-Z](?:[0-9]{1,3})?)>>%$methodCodePatch->{lc $1} // ''%ger) // die $@;
        }
    }

}
# By default, after connection failure try to reconnect every 100 ms up to 10 seconds
my $redTestObj = Redis::Fast->new( 'reconnect' => RECONNECT_UP_TO, 'every' => RECONNECT_EVERY);
sub new {
    state $exclOptions = +{ map {$_=>1} qw(on_connect redc index name client_name encoder) };
    my ($class, %options) = @_;
    
    my ($coName, $conIndex) = delete @options{qw/name index/};
    $conIndex = DFLT_REDIS_DB_N unless defined $conIndex and $conIndex !~ /[^\d]/;
    $coName //= join('_' => 'redc', 'anon', 'pid'.$$, int(rand 65536));
    exists($conoByName{$coName}) and confess sprintf(q<Cant initialize RedC: name '%s' was already reserved as a Redis connector name>, $coName);
    
    if ( $conIndex ) {
        my $nDB = databases( $redTestObj );
        confess sprintf('Redis database index #%s is out of configured bounds (min=0, max=%d)',$conIndex, $nDB - 1)
            if $conIndex >= $nDB;
    }
    my $redC;
    @options{ qw/name index redc/ } = ( $coName, $conIndex, \$redC );
    ($options{'client_name'} //= $coName . '__' . $$) =~ s%\s%_%g;
    for ( $options{'encoder'} ) {
        defined($_) && !ref($_) && length($_)
            ? length($_)>2 && substr($_, 0, 1) eq '{' && substr($_, -1, 1) eq '}'
                ? do { substr($_,0,1)=''; substr($_,1,1)=''; $_ }
                : $_
            : ($_ = DFLT_ENCODER_TAG)
    }
    my $opts = \%options;
    $redC = $class->SUPER::new(
# Hint: you can redefine "reconnection" settings in your %options passed to constructor
        'reconnect'	=>	RECONNECT_UP_TO,
        'every'		=>	RECONNECT_EVERY,
        'on_connect'	=>	sub {
            my $self = $_[0];
            __call_this_if_coderef( $opts->{'on_connect'}, $self );
            $self->SUPER::select( $opts->{'index'} );
            $self->SUPER::client_setname( $opts->{'client_name'} //= ($opts->{'name'} . '__' . $$) =~ s%\s%_%gr );
        },
        (map { $_ => $options{$_} } grep { !$exclOptions->{$_} } keys %options),
    );
    $conoByRef{refaddr $redC} = $conoByName{$coName} = $opts;
    return $redC
}

sub encoder {
    $_[1] ? $conoByRef{refaddr $_[0]}{'encoder'} = $_[1] : $conoByRef{refaddr $_[0]}{'encoder'} //= DFLT_ENCODER_TAG
}

sub select {
    my ($self, $redisDbN) = @_;
    my $p_ind = \$conoByRef{refaddr $self}{'index'};
    return $self if ${$p_ind} == $redisDbN;
    # Exception will be raised by the parent Redis::Fast class if something goes wrong, so we dont need to take care about select() result validation
    $self->SUPER::select( $redisDbN );
    ${$p_ind} = $redisDbN;
    return $self
}

sub index {
    defined($_[1]) ? $_[0]->select($_[1]) : $conoByRef{refaddr $_[0]}{'index'}
}

sub name { $conoByRef{refaddr $_[0]}{'name'} } 

sub databases {
    my $redC = shift;
    open my $fh, '<', $redC->info->{'config_file'};
    return ((local $/=<$fh>)=~m/(?:^|\n)\s*databases\s+(\d+)\s*(?:$|\n)/sm)[0];
}

sub get_redc_by {
# get_redc_by is a class-wide method that is agnostic to the passed object instance
    shift if ref $_[0] eq __PACKAGE__;
    my $byWhat = shift;
    return if ref $byWhat or !defined($byWhat);
    given ($byWhat) {
        $conoByName{$byWhat} when 'name';
        [ grep {$_->{'index'} == $byWhat} values %conoByName ] when 'index';
        default {
            confess sprintf('No such connector object attribute <<%s>>', $byWhat)
        }
    }
}

sub __call_this_if_coderef {
    defined($_[0]) and is_plain_coderef($_[0]) and $_[0]->( $#_ > 0 ? @_[1..$#_] : () )
}

sub DESTROY {
    delete $conoByName{ delete($conoByRef{refaddr $_[0]})->{'name'} };
}

1;
