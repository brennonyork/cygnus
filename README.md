  The Cygnus compiler is a multi-pass system that takes as input a configuration
  file outlining a given data type and its associated Accumulo schemas and 
  outputs a single Java file ready to ingest that data type into Accumulo. The
  primary motivation for this attempts to solve the current multi-tiered set of
  issues when regarding getting new data types into Accumulo. Cygnus is meant
  to ease the insertion of new data types into your Accumulo cluster by 
  automating the majority of work for you.

## Installation

  Because Cygnus runs on the Python interpreter it can be deployed anywhere. To
  install one only needs to run the install.sh script. To install with Protocol
  Buffer support (details below) just pass "with-protobuf" into the install.sh 
  script. The install script will ask where you would like it to install, but it
  will default to /usr/local/cygnus. You will likely need to run this as the
  "root" user.
    Ex. $ ./install.sh
        > Install directory? (default: /usr/local/cygnus)
        > < Enter >

    (Install with Protocol Buffer support)        
        $ ./install.sh with-protobuf
  
  Advanced features such as compilation into self-contained .jar
  files will be omitted without the additional requirements of Accumulo,
  Hadoop, and Zookeeper .jar files. These are automatically included if
  the correct *_HOME environment variables are present. For example:

```
$ env | grep HOME
> ACCUMULO_HOME=/usr/local/accumulo
> HADOOP_HOME=/usr/local/hadoop
> ZOOKEEPER_HOME=/usr/local/zookeeper
```

  The additional feature of Google Protocol Buffer serialization requires the 
  installation of protobufs. This code can be found at:
    http://code.google.com/p/protobuf/

## Usage

  When discussing usage it comes in two different forms. Primarily one will
  leverage this to understand how the compiler is working, but there is some
  functionality built into the Java code as well. Each of these usage scenarios
  can be printed on the command line.
    Ex. `$ cygnus --help`

    (Assuming a compiled .jar was created)
        `$ java -jar example.jar --help`

## Data Format

  The complete definition for the data format lies in the [DSL](DSL.md) file. This
  defines how to parse data in a record-by-record format using the Cygnus
  compiler.
