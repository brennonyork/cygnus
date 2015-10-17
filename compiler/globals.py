import sys

##
# VAR_CONF
#     Defines the user set-able variables for an ingest configuration.
#
# TABLE_NAME
#     -- Designates the table name to use within Accumulo
# SHARD_NUM
#     -- Denotes the amount of sharding to take place during ingest, this number
#        can greatly affect the performance of the cluster
# THREAD_NUM
#     -- Defines the number of threads to utilize when reading from a directory
# MAX_RECS
#     -- Specifies the number of individual records to write into the rfile
#        before writing it to disk
# HEADER_LEN
#     -- If there is a single header, and it should be skipped, then this value
#        can be set to the number of bytes to be skipped
# LOCAL_INPUT_DIR
#     -- Specifies the input directory for this data type
# LOCAL_OUTPUT_DIR
#     -- Specifies the local output directory before the RFiles are moved into
#        HDFS and Accumulo
# HDFS_INPUT_DIR
#     -- Used to specify the input directory if it is on HDFS and not a local
#        drive
# HDFS_OUTPUT_DIR
#     -- Specifies the HDFS output directory to house the RFiles as they are
#        imported into Accumulo
# HDFS_OUTPUT_FAIL_DIR
#     -- The HDFS path that will house any failed importation files
# USER
#     -- Defines the user field for the Accumulo user
# PASSWD
#     -- Defines the password for the above user
# ZOO_SERVERS
#     -- A comma delimited string (without whitespace) containing the hostnames
#        of the zookeepers
##
VAR = 'VAR_CONF'

##
# PARSE_CONF
#     Defines the syntax for developing a parse configuration understandable by
#     the data language
##
PARSE = 'PARSE_CONF'
                            
##
# SCHEMA_CONF
#     Determines what is held in each key within Accumulo. Currently only the
#     Row ID and Value are necessary to create a key.
# R
#     -- Row Identification
# CF
#     -- Column Family
# CQ
#     -- Column Qualifier
# CV
#     -- Column Visibility
# TS
#     -- Timestamp
# VAL
#     -- Value
##
SCHEMA = 'SCHEMA_CONF'

# Answers "Is any element in 'ls' contained in the string 's'?
# Returns a boolean as this is a predicate (hence the 'q')
def contain_q(s, ls):
    for item in ls:
        if(s.find(item) != -1): return item
        else: continue
    return False

# Given a string and a list, determines if the all items in
# the list are contained in the string
def containAll_q(s, ls):
    for item in ls:
        if(s.find(item) != -1):
            continue
        else:
            print "ERROR: Could not find", item, "in", s
            return False
    return True

# Given a dictionary and a list, determines if the dictionary
# contains only items specified in the list, but does not
# determine uniqueness
def containOnly_q(d, ls):
    for key in d:
        if(key in ls):
            continue
        else:
            print "ERROR: Invalid key (", key, ") in schema"
            return False
    return d

# Acts as map() except does not split the string into individual
# characters and, instead, acts on each logical element
def str_map(proc, ls):
    new_ls = []
    for item in ls:
        new_ls.append(proc(item))
    return new_ls

# Strips any function calls from a string and returns the root
# element and a list of functions from the outside in
def strip_func_names(s):
    ls = []
    def strip(s):
        paren = s.index("(")
        if(s[-1] == ")"):
            return (s[(paren+1):-1], s[:paren])
        else:
            print "ERROR: Could not find closing \")\" at", s
            sys.exit(-1)
    if(s.count("(") == s.count(")")):
        while("(" in s):
            s, func_name = strip(s)
            ls.append(func_name)
        return [s, ls]
    else:
        print "ERROR: Unbalanced parentheses at", s
        sys.exit(-1)
