import sys
import os
import pprint
import shutil
import subprocess as sp
import globals as globl
import templates as t
import core_passes as core
if("-pb" in sys.argv):
    protobuf = __import__("proto_passes", globals(), locals(), [], -1)

# This structure defines which functions (passes) will actually
# execute. If one were to write new passes then they must also
# be added here in the correct ordering with which they are to
# be executed. They execute in order (top to bottom). The output
# of each pass will feed the next with the initialization pass
# being the .conf file. Each pass is a list containing the function
# name as well as the type of pass it is. Currently the only
# pass types are "CORE" and "PROTOBUF". Future work will likely
# add an "OPTIMIZE" set of passes as well.
# NOTE: The second string in each list must match the variable (or
#       module name) that it came from regardless of capitalization
compiler_passes = [["load_file", "CORE"],
                   ["enforce_keys", "CORE"],
                   ["enforce_pairing", "CORE"],
                   ["inject_protobuf", "PROTOBUF"],
                   ["enumerate_terms", "PROTOBUF"],
                   ["remove_static_text", "CORE"],
                   ["apply_funcs", "CORE"],
                   ["clump_schema_vals", "CORE"],
                   ["remove_schema_text", "CORE"],
                   ["remove_lists", "CORE"],
                   ["append_shard", "CORE"],
                   ["normalize_kvpairs", "CORE"],
                   ["normalize_colvis", "CORE"],
                   ["normalize_timestamps", "CORE"],
                   ["normalize_values", "CORE"],
                   ["blanket_text", "CORE"],
                   ["finalize_schema", "CORE"],
                   ["const_label_pairing", "CORE"],
                   ["var_handler", "CORE"],
                   ["serialize_to_disk", "PROTOBUF"],]
    
# Defines all of the input variables from the Template
# that allow a substitution. This is used to ensure that
# when safe_substitution is called no value is left open to generate
# errors in the compilation
d_TEMPLATE={ "INPUT_DIR":".",
             "OUTPUT_DIR":".",
             "MAX_RECS":"10000",
             "STATIC_TEXT":"",
             "PARSE_CONF":"",
             "SCHEMA_CONF":"",
             "LOAD_CONF":"",
             "HEADER_LEN":"",
             "NUM_THREADS":"1",
             "PACKAGE_NAME":"",
             "USER_IMPORTS":"",
             "CLASS_NAME":"",
             "CONF_FILE":"",
             "JAVA_CONF_FILE":"",
             "HDFS_INPUT_Q":"false",
             "HDFS_OUTPUT_Q":"false",
             "SHARD_INC":"",
             "SHARD_NUM":"65535"}

# The default list of executable compiler passes
executable_types = ["CORE"]

# Default values for pretty printing passes
pp_passes = False
pp = pprint.PrettyPrinter(indent=2)

# Displays the help when running the compiler
def print_help():
    print
    print "CYGNUS COMPILER:"
    print
    print "  This program takes a .conf file which outlines a given data type"
    print "  and constructs a .java file to ingest said data into Accumulo"
    print "  through Key Value pairs. Additional variables can be defined within"
    print "  the .conf file to further refine how the .java file will handle the"
    print "  data in relation to cluster performance."
    print
    print "  -if or --in-file [filename]:"
    print "    Specifies the input .conf file to be read."
    print "  -of or --out-file [filename]:"
    print "    Specifies the output file. This should have a .java extension"
    print "    for proper compilation."
    print "  -c or --compile:"
    print "    Will attempt to compile the .java file into a class. This is done"
    print "    in the same directory as the output was specified. For proper"
    print "    compilation one should have ACCUMULO_HOME and HADOOP_HOME setup"
    print "    in their environment variables. If not one can always statically"
    print "    specify the class path to use with the CLASSPATH environment"
    print "    variable."
    print "  -cj or --compile-jar:"
    print "    Building on mere compilation this will attempt to create a self-"
    print "    contained executable jar. As above, the environment must be setup"
    print "    properly for this to work. Additionally, this flag will omit any"
    print "    relative paths given to the output file and will create a local"
    print "    directory structure \"./Vescori/ingest/\" to house the code. This"
    print "    directory will be removed once jar'ing is completed."
    print "  -pb or --protobuf:"
    print "    When protobufs are specified additional passes are used to"
    print "    generate Google Protobuf serialized objects representing the"
    print "    schemas outlined in the .conf file. These objects can then be"
    print "    read in by either Java, Python, or C++ for querying capabilities"
    print "    from a front end. For detailed information about the object"
    print "    layout (or to change it) take a look at the ./protobuf/cb.proto"
    print "    file."
    print "  -pp or --pretty-print:"
    print "    Adding this flag will \"pretty print\" the output of each pass"
    print "    in the compiler. Doing this might also trump any possible errors"
    print "    that might occur when parsing the .conf file."
    print "  -h or --help:"
    print "    Displays this help menu."
    print

# Determines the classpath given Cygnus plugins, environment variables, and any
# user-defined CLASSPATH
def det_classpath():
    inst_dir = os.path.dirname(os.path.dirname(os.path.realpath("/usr/bin/cygnus")))
    plugin_dir = os.path.join(inst_dir, "plugins")
    classpath = ".:"+plugin_dir+"/*"
    if("ACCUMULO_HOME" in os.environ):
        classpath += ":"+os.environ["ACCUMULO_HOME"]+"/lib/*"
    if("HADOOP_HOME" in os.environ):
        classpath += ":"+os.environ["HADOOP_HOME"]+"/*"
    if("ZOOKEEPER_HOME" in os.environ):
        classpath += ":"+os.environ["ZOOKEEPER_HOME"]+"/*"
    if("CLASSPATH" in os.environ):
        classpath += ":"+os.environ["CLASSPATH"]
    return classpath

# Determines any import statements that need to be added to the corresponding
# java code for the user-defined functions
def det_imports():
    inst_dir = os.path.dirname(os.path.dirname(os.path.realpath("/usr/bin/cygnus")))
    plugin_dir = os.path.join(inst_dir, "plugins")
    imports = {}
    for path, dirs, files in os.walk(plugin_dir):
        for f in files:
            if(os.path.splitext(f)[1] == ".class"):
                imports[os.path.splitext(os.path.relpath(os.path.join(path, f), plugin_dir))[0].replace(os.sep, '.')] = None
            elif(os.path.splitext(f)[1] == ".jar"):
                j = sp.Popen(["jar", "tf", os.path.join(path,f)], 0, None, None, sp.PIPE)
                for line in j.communicate()[0].split('\n'):
                    path, ext = os.path.splitext(line)
                    if(ext == ".class"):
                        imports[path.replace(os.sep, '.')] = None
    import_str = ""
    for key in imports:
        import_str += "import "+str(key)+";\n"
    return import_str

# This function is what iteratoes over the compiler_passes and
# determines what can run and what cannot.
def exec_compiler(ret):
    for single_pass, pass_type in compiler_passes:
        if(pass_type in executable_types):
            ret = getattr(eval(pass_type.lower()),single_pass)(ret)
        else:
            continue
        if(pp_passes):
            print single_pass # Print function name
            pp.pprint(ret)    # Print output dictionary
        if(ret): continue
        else: sys.exit(-1)
    return ret

def main(argc, argv):
    global pp_passes
    global executable_types
    omit_configs = False
    in_file = ""
    out_file = ""

    java_compile = False
    java_jar = False

    # Parse command line arguments
    i = 1
    while(i < argc):
        if((argv[i] == "-pp") or (argv[i] == "--pretty-print")): pp_passes = True
        if((argv[i] == "-h") or (argv[i] == "--help")):
            print_help()
            sys.exit(0)
        if((argv[i] == "-if") or (argv[i] == "--in-file")):
            i += 1
            in_file = argv[i]
        if((argv[i] == "-of") or (argv[i] == "--out-file")):
            i += 1
            out_file = argv[i]
        if((argv[i] == "-c") or (argv[i] == "--compile")): java_compile = True
        if((argv[i] == "-cj") or (argv[i] == "--compile-jar")):
            java_compile = True
            java_jar = True
        if((argv[i] == "-pb") or (argv[i] == "--protobuf")):
            executable_types.append("PROTOBUF")
        i += 1

    if(not in_file or not out_file):
        print "Usage: $ cygnus -if [input_file] -of [output_file] [options]"
        print "       $ cygnus -h"
        sys.exit(0)

    # Execute the compiler and update any fields not received with default values
    d_TEMPLATE.update(exec_compiler(in_file))
    class_name = (os.path.split(out_file)[1]).split('.')[0]
    d_TEMPLATE["CLASS_NAME"] = (os.path.split(out_file)[1]).split('.')[0]

    if(not java_jar):
        # Add any user-defined imports
        d_TEMPLATE["USER_IMPORTS"] = det_imports()

        with open(out_file, "w") as of:
            of.write(t.TEMPLATE.safe_substitute(d_TEMPLATE))

    if(java_compile and not java_jar):
        # Build the Java classpath from environment variables
        classpath = det_classpath()

        # Build the execution line for compilation
        compiler = ['javac',
                    '-cp', classpath,
                    out_file] 
        print "Executing: < javac -cp", classpath, out_file, ">"
        c = sp.Popen(compiler)
        c.wait()
    elif(java_compile and java_jar):
        # Create the directory structure from the relative path of execution
        print "Creating temporary directory: ./Vescori"
        proj_dir = os.getcwd()
        root_dir = os.path.join(".", "Vescori")
        if(not os.path.exists(root_dir)):
            os.makedirs(root_dir)
        os.chdir(root_dir)

        java_dir = os.path.join(".", class_name.lower())
        if(not os.path.exists(java_dir)):
            os.makedirs(java_dir)
        lib_dir = os.path.join(".", "lib")
        if(not os.path.exists(lib_dir)):
            os.makedirs(lib_dir)

        ingest_file = os.path.join(java_dir, os.path.split(out_file)[1])

        # Build the Java classpath (from environment variables if found)
        classpath = det_classpath()
        if("ACCUMULO_HOME" in os.environ and
           "HADOOP_HOME" in os.environ and
           "ZOOKEEPER_HOME" in os.environ):
            omit_configs = False
        else:
            print "WARN: Could not find {ACCUMULO|HADOOP|ZOOKEEPER}_HOME in the environment."
            print "      -- Omitting site .xml configuration imports"
            omit_configs = True

        # Unzip necessary jars
        print "Inflating:", classpath.split(':'), "to ./Vescori/"
        for item in classpath.split(':'):
            walk_depth = 0
            for path, dirs, files in os.walk(item.split('*')[0]):
                if(walk_depth >= 1):
                    break
                for f in files:
                    if(os.path.splitext(f)[1] == ".jar"):
                        z = sp.Popen(["jar", "-xf", os.path.join(path,f)])
                        z.wait()
            walk_depth += 1

        # Grab any .xml files relevant to site configuration
        if(not omit_configs):
            for item in (os.environ["ACCUMULO_HOME"]+"/conf/:"+os.environ["HADOOP_HOME"]+"/conf/"+os.environ["ZOOKEEPER_HOME"]+"/conf/").split(':'):
                walk_depth = 0
                for path, dirs, files in os.walk(item.strip()):
                    if(walk_depth >= 1):
                        break;
                    for f in files:
                        if(os.path.splitext(f)[1] == ".xml"):
                            shutil.copy2(os.path.join(path,f), os.getcwd())
                walk_depth += 1

        # Build the execution line for compilation
        compiler = ['javac',
                    '-cp', ".",
                    ingest_file]

        # Add the package into the Java file
        d_TEMPLATE["PACKAGE_NAME"] = "package "+class_name.lower()+";\n"
        print "Setting package: Vescori."+class_name.lower()

        # Add any user-defined imports
        d_TEMPLATE["USER_IMPORTS"] = det_imports()

        # Write the Ingest code into ./cygnus/ingest/
        with open(ingest_file, "w") as of:
            of.write(t.TEMPLATE.safe_substitute(d_TEMPLATE))

        print "Executing: <",
        for item in compiler:
            print item,
        print ">"

        # Run the javac compiler
        c = sp.Popen(compiler)
        c.wait()

        # Set the manifest file
        print "Generating file: ./Vescori/manifest.mf"
        manifest_file = os.path.join(".", "manifest.mf")
        with open(manifest_file, "w") as manifest:
            manifest.write("Main-Class: "+class_name.lower()+"."+d_TEMPLATE["CLASS_NAME"]+"\n")
            manifest.write("Class-Path: .\n")

        jar = ['jar',
               'cmf', 
               "manifest.mf", # Define the manifest file
               os.path.join("..",os.path.splitext(os.path.split(in_file)[1])[0]+".jar"), # Name the jar
               "."]
        
        print "Executing: <",
        for item in jar:
            print item,
        print ">"

        j = sp.Popen(jar)
        j.wait()
        
        os.chdir(proj_dir)

        rm = ["rm", "-rf", root_dir]

        print "Executing: <",
        for item in rm:
            print item,
        print ">"
        
        r = sp.Popen(rm)
        r.wait()

if __name__ == "__main__":
    try:
        main(len(sys.argv), sys.argv)
    except KeyboardInterrupt:
        print "Caught interrupt, cleaning up..."
