#!/bin/bash

# Determine the install directory
INST_DIR=/usr/local/cygnus
echo "Install directory? (default: /usr/local/cygnus)"
read USER_INST_DIR
if [ -n "$USER_INST_DIR" ]; then
    INST_DIR=$USER_INST_DIR
fi

# Remove any old instances
rm -rf $INST_DIR

# Build the directory structure
mkdir -p $INST_DIR/bin
mkdir -p $INST_DIR/plugins
cp -R ./compiler $INST_DIR
cp -R ./protobuf $INST_DIR
chmod 777 $INST_DIR/plugins

# Create a wrapper runtime environment
RUNTIME=$INST_DIR/bin/runtime
echo "#!/bin/bash" > $RUNTIME
echo "PYTHON=\`which python\`" >> $RUNTIME
echo "\$PYTHON $INST_DIR/compiler/cygnus.py \$*" >> $RUNTIME
chmod +x $RUNTIME

# Link the runtime symbolically to /usr/bin/cygnus
rm -f /usr/bin/cygnus
ln -s -T $RUNTIME /usr/bin/cygnus

# Build protobuf support if included
case "$1" in
    with-protobuf)
	protoc --proto_path=$INST_DIR/protobuf --python_out=$INST_DIR/compiler/. $INST_DIR/protobuf/cb.proto
	;;
    *)
	;;
esac
