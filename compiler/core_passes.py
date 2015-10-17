import globals as globl
import templates as t
import os

# Loads the given .conf file in accordance with the defined grammar
# in a line-by-line fashion. This is the first pass in the compiler.
#
# INPUT:
#     Takes as input a .conf file conforming to the language as
#     defined by the DATA LANGUAGE STANDARD found in dls.txt
#
# OUTPUT:
#     { <globl.VAR> : {varName : varValue, ...},
#       <globl.PARSE> : [(const, label), ...],
#       <globl.SCHEMA> : [{'R' : cbValue, 
#                          'VAL' : cbValue, 
#                          cbCol : cbValue, ...}, ...]}
def load_file(f):
    d = {}
    d[globl.SCHEMA] = []
    d[globl.VAR] = {}
    d["CONF_FILE"] = ""
    d["JAVA_CONF_FILE"] = ""
    parse_line = ""

    # As new VAR_CONF variables are defined, they are
    # going to need to be added to the var_guard as well
    # as providing the necessary boolean guard types
    var_guard = {'TABLE_NAME':lambda x: (x.isalnum()),
                 'SHARD_NUM':lambda x: (x.isdigit() and (int(x) > 0) and (int(x) <= 65535)),
                 'THREAD_NUM':lambda x: (x.isdigit() and (int(x) > 0)),
                 'RECORD_NUM':lambda x: (x.isdigit() and (int(x) > 0)),
                 'HEADER_LEN':lambda x: (x.isdigit() and (int(x) > 0)),
                 'LOCAL_INPUT_DIR':lambda x: os.access(x, os.R_OK),
                 'LOCAL_OUTPUT_DIR':lambda x: os.access(x, os.W_OK),
                 'HDFS_INPUT_DIR':lambda x: os.path.isabs(x),
                 'HDFS_OUTPUT_DIR':lambda x: os.path.isabs(x),
                 'HDFS_OUTPUT_FAIL_DIR':lambda x: os.path.isabs(x),
                 'USER':lambda x: (x.isalpha()),
                 'PASSWD': lambda x: x,
                 'ZOO_SERVERS': lambda x: x,
                 'ZOO_INSTANCE': lambda x: (x.isalpha()),
                 'AUGMENT_DIR':lambda x: os.access(x, os.R_OK),}

    def validate_var(s):
        val = var_q(s)
        var = globl.str_map(lambda x: x.strip(), s.split('='))
        if(val and (len(var) == 2)):
            if(var_guard[val](var[1])):
                return var
            else:
                print "ERROR: Invalid value for variable", var[0]
                return False
        else:
            print "ERROR: Could not properly split", var
            return False

    # Given a string, it ensures that the string contains a valid
    # variable definition
    def var_q(s):
        # Build available variable names from the variable guard
        avail_vars = []
        for key in var_guard:
            avail_vars.append(key)
        return globl.contain_q(s, avail_vars)

    const_guard = lambda x: ((type(x) is list and 
                              (x[0] == 'S')) or
                             (type(x) is str and 
                              (x.isdigit() or 
                               label_guard(x))))
    def label_guard(s):
        for i in range(0, len(s)):
            if(s[i].isalpha() or
               (s[i] == '_') or
               (s[i] == '-')):
                continue
            else:
                return False
        return True
    
    # Validates a parse line and returns, in order, the
    # const label pairs for easy iteration
    def validate_parse(s):
        parse = []
        i = 0
        while(i < len(s)):
            if(s[i] != '%'):
                print "ERROR: Did not see specified '%', but saw", s[i]
                return False
            else:
                const = ""
                i += 1
                if(s[i] == 'S'): # If a variable length is defined
                    const = ['S',""]
                    i += 1
                    # Ensure directly after the S is a tick mark
                    if(s[i] == '"'):
                        const[1] += s[i]
                        i += 1
                        # Loop until we find the next tick mark. This has no
                        # guard purposefully to allow any arbitrary byte to be
                        # properly read and represented.
                        while(i < len(s)):
                            if(s[i] == '\\'):
                                const[1] += s[i]
                                const[1] += s[(i+1)]
                                i += 2
                            if(s[i] == '"'):
                                const[1] += s[i]
                                i += 1
                                break
                            else:
                                const[1] += s[i]
                                i += 1
                    else:
                        print "ERROR: No quote (\"\"\") after string declaration"
                        return False
                else: # Type of const is a string
                    for j in range(i, len(s)):
                        if(const_guard(s[j])):
                            const += s[j]
                            i = (j + 1)
                        else:
                            break
                    if((type(const) is str) and 
                       (const == "")):
                        print "ERROR: Could not find Constant after '%'"
                        return False
                if(not const_guard(const)):
                    print "ERROR:", const, "is not an allowable Constant"
                    return False
                # Parse the label after the constant defined with brackets
                if((i < len(s)) and
                   (s[i] == '[')):
                    label = s[i]
                    for j in range((i + 1), len(s)):
                        if(label_guard(s[j])):
                            label += s[j]
                            i = (j + 1)
                        elif(s[j] == ']'):
                            label += s[j]
                            i = (j + 1)
                            break
                        else:
                            print "ERROR: Incorrect syntax for Label", label
                            return False
                    if((label[0] == '[') and
                       (label[-1] == ']')):
                        parse.append((const, label[1:-1]))
                    else:
                        print "ERROR: Failure to parse Label", label
                        return False
                else:
                    parse.append((const, False))
        return parse

    schema_guard = {'R':lambda x: (schema_label_guard(x)),
                    'CF':lambda x: (schema_label_guard(x)),
                    'CQ':lambda x: (schema_label_guard(x)),
                    'CV':lambda x: (schema_label_guard(x)),
                    'TS':lambda x: (schema_label_guard(x)),
                    'VAL':lambda x: (schema_label_guard(x))}
    def schema_label_guard(s):
        quoted = False
        for i in range(0, len(s)):
            if(s[i].isalpha() or
               (s[i] == '_') or
               (s[i] == '-') or
               (s[i] == '+')):
                continue
            elif(quoted):
                if(s[i] == '"'):
                    quoted = False
                else:
                    continue
            elif(s[i] == '"'):
                quoted = True
                continue
            else:
                return False
        if(quoted):
            print "ERROR: Failed to find closing quote (\") in", s
            return False
        return True

    def validate_schema(s):
        val = schema_q(s)
        avail_schema = ['R', 'CF', 'CQ', 'CV', 'TS', 'VAL']
        d_var = {}
        if(val):
            # Tokenize the schema line
            for kv in globl.str_map(lambda x: x.strip(), s.split(' ')):
                # Tokenize the key value pairs
                var = globl.str_map(lambda x: x.strip(), kv.split(':'))
                if(len(var) == 2):
                    if(var[0] in avail_schema):
                        # Determine any function calls (validate later)
                        if('(' in var[1]):
                            for term in globl.str_map(lambda x: x.strip(), var[1].split('+')):
                                term, func_names = globl.strip_func_names(term)
                                if(schema_guard[var[0]](term)):
                                    continue
                                else:
                                    print "ERROR: Invalid value for", var[0], "being", var[1]
                                    return False
                            d_var[var[0]] = var[1]
                        elif(schema_guard[var[0]](var[1])):
                            d_var[var[0]] = var[1]
                        else:
                            print "ERROR: Invalid value for", var[0], "being", var[1]
                            return False
                    else:
                        print "ERROR: Invalid key (", var[0], ") in schema"
                        return False
                else:
                    print "ERROR: Could not properly split", var
                    return False
            return d_var
        else:
            return False

    # Checks the string to ensure it has the minimum requirements
    # to be considered a Schema
    def schema_q(s):
        reqs = ['R:', 'VAL:']
        return globl.containAll_q(s, reqs)

    with open(f, "r") as f:
        for line in f:
            # Before all else insert the line into the comments of the java code
            # and generate the function to display the .conf from command line
            # of the java executable.
            d["CONF_FILE"] += line
            d["JAVA_CONF_FILE"] += "System.out.println(\""+line.strip().replace('\\', '\\\\').replace('"','\\"')+"\");\n"

            # Do some preprocessing of the line
            line = (line.split('#')[0]).strip() # Remove any comments

            # If we have a global variable then add it to
            # the dictionary of variable value pairs
            if('=' in line):
                ret = validate_var(line)
                if(ret): d[globl.VAR][ret[0]] = ret[1]
                else: return False
            # If we have a parse configuration line then add
            # it to the global line
            elif('%' in line):
                parse_line += line
            # If we have a schema definition then append it
            # to the list of schemas
            elif(':' in line):
                ret = validate_schema(line)
                if(ret): d[globl.SCHEMA].append(ret)
                else: return False            
            # Else we have a line we don't have a validator
            # definition for (likely a blank line)
            else:
                pass
        # Once the parse line is accumulated, run it through
        # the validator to determine its worthiness
        ret = validate_parse(parse_line)
        if(ret): d[globl.PARSE] = ret
        else: return False

    return d

# Acts upon the SCHEMA of the dictionary to enforce specific
# Accumulo Key constructors
def enforce_keys(d):
    # Iterate over each unique schema
    for dict in d[globl.SCHEMA]:
        if('CQ' in dict):
            if('CF' in dict): continue
            else:
                print "ERROR: Found a CQ without a CF in", dict
                return False
        else: continue
    return d

# Enforces each value in the SCHEMA as a defined variable
# in the PARSE
def enforce_pairing(d):
    found_q = False
    for dict in d[globl.SCHEMA]:
        for key in dict:
            for item, ls in map(globl.strip_func_names, dict[key].split('+')):
                # No need to check for empty strings or statically
                # assigned strings (e.g. "IPS")
                if((item == "") or
                   ((item != "") and 
                    (item[0] == '"') and
                    (item[-1] == '"'))):
                    continue
                else:
                    for key, value in d[globl.PARSE]:
                        if(item == value):
                            found_q = True
                            break
                        else: continue
                    if(not found_q):
                        print "ERROR: no matching variable in PARSE line for", item
                        return False
                    else:
                        found_q = False
                        continue
    return d

# Removes any static text definitions and replaces them with Hadoop
# Text. Assigns a new variable to them and replaces that within the
# SCHEMA.
def remove_static_text(d):
    d['STATIC_TEXT'] = {}
    for dict in d[globl.SCHEMA]:
        for key in dict:
            dict[key] = map(globl.strip_func_names, globl.str_map(lambda x: x.strip(), dict[key].split('+')))
            for i in range(0, len(dict[key])):
                # If we have a static text
                if((dict[key][i][0] != "") and
                   ((dict[key][i][0][0] == '"') and
                    (dict[key][i][0][-1] == '"'))):
                    new_item = "_"+dict[key][i][0][1:-1]
                    # Instead of consolidating here we leverage the dictionary
                    # object to 'unique-ify' all text objects
                    d['STATIC_TEXT'][new_item] = t.TEXT.safe_substitute(var=new_item,
                                                                        val=dict[key][i][0])
                    dict[key][i][0] = new_item
    text_str = ""
    for key in d['STATIC_TEXT']:
        text_str += d['STATIC_TEXT'][key]
    d['STATIC_TEXT'] = text_str
    return d

def apply_funcs(d):
    version = 0
    for dict in d[globl.SCHEMA]:
        version += 1
        for key in dict:
            iter = 0
            for term, funcs in dict[key]:
                if(funcs != []):
                    root = term+str(key)+str(version)+"_"
                    my_str = t.TEXT.safe_substitute(var=root,
                                                    val=term)
                    my_funcs = ""
                    funcs.reverse()
                    for i in range(0, len(funcs)):
                        if(i == 0):
                            my_funcs = funcs[i]+"("+root+")"
                        else:
                            my_funcs = funcs[i]+"("+my_funcs+")"
                    my_funcs += ";\n"
                    my_str += root+" = "+my_funcs
                    dict[key][iter] = [root, my_str]
                else:
                    dict[key][iter] = dict[key][iter][0]
                iter += 1
    return d                    

# For any value with more than a single variable, this pass will
# consolidate those into a single string of Java defining the root
# and all appends onto the root that need to be made.
def clump_schema_vals(d):
    version = 0
    for dict in d[globl.SCHEMA]:
        version += 1
        for key in dict:
            if(len(dict[key]) > 1):
                my_str = ""
                root = "_"+dict[key][0]+str(key)+str(version)
                for i in range(0, len(dict[key])):
                    if(type(dict[key][i]) is list):
                        my_str += dict[key][i][1]
                        dict[key][i] = dict[key][i][0]
                for i in range(0, len(dict[key])):
                    if(i == 0):
                        my_str += t.TEXT.safe_substitute(var=root,
                                                      val=(str(dict[key][i])))
                    else:
                        my_str += t.APPEND.safe_substitute(var1=root,
                                                        var2=dict[key][i])
                dict[key] = [root, my_str]
            else:
                if(type(dict[key][0]) is list):
                    my_str = dict[key][0][1]
                    dict[key] = [dict[key][0][0],]
                    dict[key].append(my_str)
    return d

# Removes the text from the previous pass and places it into
# a separate 'TEXT' key within the dictionary. The root variable
# is left within the SCHEMA value.
def remove_schema_text(d):
    for dict in d[globl.SCHEMA]:
        my_str = ""
        for key in dict:
            if(len(dict[key]) == 2):
                my_str += dict[key][1]
                dict[key] = [dict[key][0],]
        if(my_str != ""):
            dict['TEXT'] = my_str
    return d

# Remove all values that are still contained as a list
def remove_lists(d):
    kv_list = ['VAL', 'TS', 'CV', 'CQ', 'CF', 'R']
    for dict in d[globl.SCHEMA]:
        for key in kv_list:
            if((key in dict) and 
               (len(dict[key]) == 1)):
                dict[key] = dict[key][0]
    return d

# This pass will determine if sharding needs to take place and, if so, will
# increment the shard and append that value to the rowID
def append_shard(d):
    if("SHARD_NUM" in d[globl.VAR]):
        d["SHARD_INC"] = "incShard();"
        version = 0
        for dict in d[globl.SCHEMA]:
            version += 1
            if(dict['R'][0] == '_'): # We already have a unique rowID
                dict['TEXT'] += t.APPEND.safe_substitute(var1=dict['R'],
                                                         var2="sb")
            else: # We need to create a unique rowID
                root = "_"+dict['R']+'R'+str(version)
                dict['TEXT'] = dict.get('TEXT', '') + t.TEXT.safe_substitute(var=root, val=dict['R'])
                dict['R'] = root
                dict['TEXT'] += t.APPEND.safe_substitute(var1=dict['R'],
                                                         var2="sb")
    return d

# Add back any SCHEMA values not present previously
# with an empty string to simplify safe_substitution
def normalize_kvpairs(d):
    kv_list = ['VAL', 'TS', 'CV', 'CQ', 'CF', 'R']
    for dict in d[globl.SCHEMA]:
        for key in kv_list:
            if(key not in dict):
                dict[key] = ""
    return d

# This pass will leverage the ColumnVisibility object to
# ensure that each CV is properly allowed in Accumulo
def normalize_colvis(d):
    for dict in d[globl.SCHEMA]:
        if(('CV' in dict) and (dict['CV'] != '')):
            dict['CV'] = "new Text(new ColumnVisibility("+dict['CV']+".getBytes()).flatten())"
    return d

# Ensure that any timestamp runs through the bytesToLong function
# internally defined to ensure proper truncation / padding of bytes
def normalize_timestamps(d):
    for dict in d[globl.SCHEMA]:
        if(('TS' in dict) and (dict['TS'] != '')):
            dict['TS'] = "bytesToLong("+dict['TS']+")"
    return d

# Ensure a proper Value field
def normalize_values(d):
    for dict in d[globl.SCHEMA]:
        if(dict['VAL'] != ''):
            dict['VAL'] = dict['VAL']+".getBytes()"
        else:
            dict['VAL'] = "new byte[0]"
    return d

# Add in blanket Text objects for unused CQ and CF fields. This is
# needed if someone were to only specify a Row and Column Visibility.
# Since CV's are very important for future deployment we must ensure
# a user can specify a CV under any condition.
def blanket_text(d):
    kv_list = ['CQ', 'CF', 'R']
    for dict in d[globl.SCHEMA]:
        for key in kv_list:
            if(dict[key] == ""):
                dict[key] = "new Text()"
    return d

# Removes all instances of 'R', 'CF', etc. in place of a single text
# containing the Java invocation methods.
def finalize_schema(d):
    # Order matters! They must be in reverse order that the Accumulo Key
    # method needs
    key_list = ['TS', 'CV', 'CQ', 'CF', 'R']
    kv_list = ['VAL', 'TS', 'CV', 'CQ', 'CF', 'R']

    # Used to properly append a comma onto each item except
    # the last. Only operates on the list of key elements
    # and not the value
    def add_comma(d, ls):
        for key in ls:
            if(key not in d):
                continue
            elif(d[key] != ""):
                d[key] = d[key]+","

    for dict in d[globl.SCHEMA]:
        if(('TS' in dict) and (dict['TS'] != '')):
            add_comma(dict, key_list[(key_list.index('TS')+1):])
        elif(('CV' in dict) and (dict['CV'] != '')):
            add_comma(dict, key_list[(key_list.index('CV')+1):])
        elif(('CQ' in dict) and (dict['CQ'] != '')):
            add_comma(dict, key_list[(key_list.index('CQ')+1):])
        elif(('CF' in dict) and (dict['CF'] != '')):
            add_comma(dict, key_list[(key_list.index('CF')+1):])

    # Add the full 'KEY' to the dictionary
    key_str = ""
    text_str = ""
    for dict in d[globl.SCHEMA]:
        key_str += t.KEYVALUE.safe_substitute(dict)
        text_str += dict.get('TEXT', '')
    d[globl.SCHEMA] = text_str+key_str

    return d

# Change the tuples from PARSE into Java code and concatenate
# them into a single string
def const_label_pairing(d):
    parse_str = ""
    for const, label in d[globl.PARSE]:
        if(type(const) is list and const[0] == 'S'):
            parse_str += t.STR_LABEL.safe_substitute(label=label,
                                                     delim=const[1])
        elif(label and (type(const) is str)):
            parse_str += t.CONST_LABEL.safe_substitute(const=const,
                                                       label=label)
        elif(not label):
            parse_str += t.ANON_LABEL.safe_substitute(const=const)
        else:
            print "ERROR: Could not understand Constant", const, "and / or Label", label
            return False
    d[globl.PARSE] = parse_str
    return d

# This is the generic pass to handle any / all variables that were
# defined in the .conf file.
def var_handler(d):
    if("THREAD_NUM" in d[globl.VAR]):
        d["NUM_THREADS"] = d[globl.VAR]["THREAD_NUM"]
    if("HEADER_LEN" in d[globl.VAR]):
        d["HEADER_LEN"] = t.HEADER_SKIP.safe_substitute(HEADER_LEN=d[globl.VAR]["HEADER_LEN"])
    if("SHARD_NUM" in d[globl.VAR]):
        d["SHARD_NUM"] = d[globl.VAR]["SHARD_NUM"]
    if("RECORD_NUM" in d[globl.VAR]):
        d["MAX_RECS"] = d[globl.VAR]["RECORD_NUM"]
    if("LOCAL_INPUT_DIR" in d[globl.VAR]):
        d["INPUT_DIR"] = d[globl.VAR]["LOCAL_INPUT_DIR"]
    if("LOCAL_OUTPUT_DIR" in d[globl.VAR]):
        d["OUTPUT_DIR"] = d[globl.VAR]["LOCAL_OUTPUT_DIR"]
    if("HDFS_INPUT_DIR" in d[globl.VAR]):
        d["INPUT_DIR"] = d[globl.VAR]["HDFS_INPUT_DIR"]
        d["HDFS_INPUT_Q"] = "true"
    if("HDFS_OUTPUT_DIR" in d[globl.VAR]):
        d["OUTPUT_DIR"] = d[globl.VAR]["HDFS_OUTPUT_DIR"]
        d["HDFS_OUTPUT_Q"] = "true"
    if("ZOO_SERVERS" in d[globl.VAR] and
       "ZOO_INSTANCE" in d[globl.VAR] and
       "USER" in d[globl.VAR] and
       "PASSWD" in d[globl.VAR] and
       "TABLE_NAME" in d[globl.VAR] and
       "HDFS_OUTPUT_DIR" in d[globl.VAR] and
       "HDFS_OUTPUT_FAIL_DIR" in d[globl.VAR]):
        d["LOAD_CONF"] = t.LOAD_CONF.safe_substitute(d[globl.VAR])
    return d
