#!/bin/bash

##
# Author: Brennon York
# Email:  brennon.york@gmail.com
##
# NOTE: This benchmarking system is setup be executed from the /benchmark folder
#       of the root cygnus/ directory.
##

# Overloaded variable to denote how the bash 'time' function operates
TIMEFORMAT='%3R\t%3U\t%3S'

COMPILER="../compiler/cygnus.py"
CONF_DIR=`find ./conf -name '*.conf'`
OUT_DIR="./output"
WITH_PROTO=0
VERBOSE=0
PASSES="123"
ITER=5

while (( "$#" )); do
    case "$1" in
	--with-proto)
	    WITH_PROTO=1
	    shift
	    ;;
	-v | --verbose)
	    VERBOSE=1
	    shift
	    ;;
	-p | --passes)
	    shift
	    if [ -n "$1" ]; then 
		PASSES=$1
	    else
		echo "ERROR: No passes specified with '-p' flag."
		exit
	    fi
	    shift
	    ;;
	-h | --help)
	    echo "CYGNUS BENCHMARKING SYSTEM"
	    echo
	    echo "  This program is meant to validate the Cygnus compiler through three separate"
	    echo "  phases."
	    echo "   1 -- Input Validation for Configuration Files"
	    echo "   2 -- Input Validation for the Cygnus Compiler"
	    echo "   3 -- Execution Benchmarking and Timing"
	    echo
	    echo "  -v | --verbose:"
	    echo "    Will be much more verbose in its output. Good for debugging and testing"
	    echo "    new additions to the benchmarking system."
	    echo "  -p | --passes [passes]:"
	    echo "    Defines which set of passes to perform. This is input as a string after"
	    echo "    the flag. (Default: \"123\")"
	    echo "      Ex. ./exec_bench.sh -p \"12\" will only perform input validation"
	    echo "      Ex. ./exec_bench.sh -p \"3\" will only execute timing benchmarks"
	    echo "  --iterations [iterations]:"
	    echo "    This only applies to the third phase and defines the number of iterations"
	    echo "    in which to run the Ingest Engines that Cygnus outputs. (Default: 5)"
	    echo "  --with-proto:"
	    echo "    Enables Phase II to test the creation of Protocol Buffers for schema"
	    echo "    serialization."
	    echo "  -h | --help:"
	    echo "    Prints this he4lp screen."
	    echo "  usage:"
	    echo "    Prints the default usage for this program."
	    exit
	    ;;
	--iterations)
	    shift
	    if [ -n "$1" ]; then 
		ITER=$1
	    else
		echo "ERROR: No iteration number specified with '--iterations' flag."
		exit
	    fi
	    shift
	    ;;
	usage)
	    echo "Usage: $ ./exec_bench.sh [-v|--verbose] [[-p|--passes] <PASS_NUMS>] [--with-proto]"
	    exit
	    ;;
	*)
	    echo "Usage: $ ./exec_bench.sh [-v|--verbose] [[-p|--passes] <PASS_NUMS>] [--with-proto]"
	    exit
	    ;;
    esac
done

##
# Benchmarking (Phase I)
##
# The goal is to run the compiler through a multitude of input validation
# checks for each variable allowable within the configuration files.
##

function phase_i {

    echo
    echo "PHASE I"
    echo

    LOOP=102 # Magic number to determine the number of lines to eat to cleanly
             # validate each variable
    BAD_CONF=./conf/bad_silk.bench

    ## Make a copy of the bad_silk.conf file
    cp ./conf/bad_silk.conf $BAD_CONF

    [ $VERBOSE -eq 0 ] && echo -n "Input Validation -> "
    while [ $LOOP -gt 0 ]
    do
	CUR_LINE=`head -1 $BAD_CONF`
	if [[ $CUR_LINE =~ ^(##)+|^([^=])*$ ]] ; then
	    sed -i '1d;' $BAD_CONF
	    LOOP=$(( $LOOP - 1 ))
	    continue
	fi
	[ $VERBOSE -gt 0 ] && echo -n "$CUR_LINE -> "
	python $COMPILER -if $BAD_CONF -of $OUT_DIR/Raw.java &>/dev/null
	if [ $? -ne 0 ]; then
	    [ $VERBOSE -gt 0 ] && echo "PASSED"
	else
	    echo "FAILED" && exit
	fi
	sed -i '1d;' $BAD_CONF
	LOOP=$(( $LOOP - 1 ))
    done
    [ $VERBOSE -eq 0 ] && echo "PASSED"
    [ -e $BAD_CONF ] && rm -f $BAD_CONF

}

##
# Benchmarking (Phase II)
##
# The goal is to stress the compiler by running it through a gamut of possible
# compile options to ensure proper error handling.
##

function phase_ii {
    
    echo
    echo "PHASE II"
    echo

    # Arg_1: Configuration File
    # Arg_*: Flags to the compiler
    function execute {
	CONF=$1
	shift
	[ $VERBOSE -gt 0 ] && echo -n "$CONF $* -> "
	RET=`(python $COMPILER -if $CONF -of $OUT_DIR/Raw.java $* 1>/dev/null) 2>&1`
	[[ -n "$RET" ]] || [[ $? -eq 0 && $CONF =~ bad_silk ]] && echo "FAILED" && echo $RET && exit
	[ $VERBOSE -gt 0 ] && echo "PASSED"
    }
    
    for CONF in $CONF_DIR
    do
	rm -f $OUT_DIR/*
	[ $VERBOSE -eq 0 ] && echo -n "CONF FILE: $CONF -> "
	execute $CONF
	execute $CONF -c
	execute $CONF -cj
	if [ $WITH_PROTO -gt 0 ]; then
	    execute $CONF -pb
	    execute $CONF -c -pb
	    execute $CONF -cj -pb
	fi
	[ -e *.jar ] && mv *.jar $OUT_DIR/
	[ -e *.pb ] && mv *.pb $OUT_DIR/
	[ $VERBOSE -eq 0 ] && echo "PASSED"
    done

    rm -f $OUT_DIR/*
}

##
# Benchmarking (Phase III)
##
# The goal of this phase is to stress the generated ingestion engines on a wide
# range of inputs with binary and ascii-based formats. These passes will
# include benchmarking of:
#   -- Local Input (Directory) -> Local Output
#   -- Local Input (Directory) -> HDFS Output
#   -- Local Input (Streaming) -> Local Output
#   -- Local Input (Streaming) -> HDFS Output
#   -- HDFS Input (Directory)  -> Local Output
#   -- HDFS Input (Directory)  -> HDFS Output
##

function phase_iii {
    
    echo
    echo "PHASE III"
    echo

    # Arg_1: Configuration file
    function cygnus {
	[ $VERBOSE -gt 0 ] && echo -n "Building Ingest Engine -> "
	python $COMPILER -if $1 -of $OUT_DIR/Bench.java -cj &>/dev/null
	[ -e *.jar ] && mv *.jar $OUT_DIR/
	[ $VERBOSE -gt 0 ] && echo "COMPLETE"
    }

    # Arg_1: A METRICS variable containing 'time' output with newline delimiters
    function compute_stats {
	echo -e $1 | awk -v iter=$ITER -F' ' \
	    'BEGIN { REAL=0; SYS=0; USR=0; \
                     print "\treal\tsys\tuser"} \
             { REAL += $1; SYS += $2; USR += $3; \
               if(REAL_MIN==""){ REAL_MIN=$1; REAL_MAX=$1; } \
               if(SYS_MIN==""){ SYS_MIN=$2; SYS_MAX=$2; } \
               if(USR_MIN==""){ USR_MIN=$3; USR_MAX=$3; } \
               if($1 >= REAL_MAX && $1 != "") REAL_MAX=$1; \
               if($2 >= SYS_MAX && $2 != "") SYS_MAX=$2; \
	       if($3 >= USR_MAX && $3 != "") USR_MAX=$3; \
               if($1 < REAL_MIN && $1 != "") REAL_MIN=$1; \
               if($2 < SYS_MIN && $2 != "") SYS_MIN=$2; \
               if($3 < USR_MIN && $3 != "") USR_MIN=$3; } \
             END { print "max\t"REAL_MAX"\t"SYS_MAX"\t"USR_MAX"\n" \
                         "avg\t"(REAL/iter)"\t"(SYS/iter)"\t"(USR/iter)"\n" \
                         "min\t"REAL_MIN"\t"SYS_MIN"\t"USR_MIN }'
    }

    # Arg_1: The $VESCORI .jar to execute
    # Arg_2: Which benchmark execution to run
    # Arg_3: File to cat in if benchmarking the streaming option
    function exec_star {
	LOCAL_ITER=$ITER
	VESCORI=$1
	METRICS=
	# Determine IO directories through the 'dump configuration' option
	LID=`java -jar $VESCORI -dc 2>/dev/null | grep LOCAL_INPUT_DIR | awk -F'=' '{print $2}' | tr -d ' #'`
	LOD=`java -jar $VESCORI -dc 2>/dev/null | grep LOCAL_OUTPUT_DIR | awk -F'=' '{print $2}' | tr -d ' #'`
	HID=`java -jar $VESCORI -dc 2>/dev/null | grep HDFS_INPUT_DIR | awk -F'=' '{print $2}' | tr -d ' #'`
	HOD=`java -jar $VESCORI -dc 2>/dev/null | grep HDFS_OUTPUT_DIR | awk -F'=' '{print $2}' | tr -d ' #'`
	[ $VERBOSE -eq 0 ] && echo -n "Executing: $VESCORI ($2) -> "
	while [ $LOCAL_ITER -gt 0 ]
	do
	    [ $VERBOSE -gt 0 ] && echo -n "Executing: $VESCORI ($2) -> "
	    case "$2" in
                # Local Input (Directory) -> Local Output
		lid2lod) METRICS=`time (java -jar $VESCORI -lid $LID -lod $LOD &>/dev/null) 2>&1`"\n"$METRICS ;;
                # Local Input (Directory) -> HDFS Output
		lid2hod) METRICS=`time (java -jar $VESCORI -lid $LID -hod $HOD &>/dev/null) 2>&1`"\n"$METRICS ;;		   
                # Local Input (Streaming) -> Local Output
		str2lod) METRICS=`time (cat $3 | java -jar $VESCORI -s -lod $LOD &>/dev/null) 2>&1`"\n"$METRICS ;;
                # Local Input (Streaming) -> HDFS Output
		str2hod) METRICS=`time (cat $3 | java -jar $VESCORI -s -hod $HOD &>/dev/null) 2>&1`"\n"$METRICS ;;
                # HDFS Input (Directory)  -> Local Output
		hid2lod) METRICS=`time (java -jar $VESCORI -hid $HID -lod $LOD &>/dev/null) 2>&1`"\n"$METRICS ;;
                # HDFS Input (Directory)  -> HDFS Output
		hid2hod) METRICS=`time (java -jar $VESCORI -hid $HID -hod $HOD &>/dev/null) 2>&1`"\n"$METRICS ;;
	    esac
	    LOCAL_ITER=$(( $LOCAL_ITER - 1 ))
	    [ $VERBOSE -gt 0 ] && echo "COMPLETE"
	done
	[ $VERBOSE -eq 0 ] && echo "PASSED"
	echo
	[ -n "$METRICS" ] && compute_stats $METRICS
	echo
    }

    # Arg_1: Path to .conf file from benchmark/.
    # Arg_2: File to stream in for stream benchmarking
    function bench_star {
	rm -f $OUT_DIR/*

	CONF=$1
	cygnus $CONF
	VESCORI=`find $OUT_DIR -name '*.jar'`

	exec_star $VESCORI lid2lod "$2"
	exec_star $VESCORI str2lod "$2"
	if [ -n "`which hadoop 2>/dev/null`" ]; then # If 'hadoop' is available, test it
	    exec_star $VESCORI lid2hod "$2"
	    exec_star $VESCORI str2hod "$2"
	    exec_star $VESCORI hid2lod "$2"
	    exec_star $VESCORI hid2hod "$2"
	fi

	rm -f $OUT_DIR/*
    }

    bench_star "./conf/silk.conf" ./binary/silk_repo/500k.rwf
    bench_star "./conf/palo-alto-access.conf" ./ascii/palo_alto_repo/palo-alto-access.log

}

[[ $PASSES =~ 1 ]] && phase_i
[[ $PASSES =~ 2 ]] && phase_ii
[[ $PASSES =~ 3 ]] && phase_iii
