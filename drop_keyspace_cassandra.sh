#!/usr/bin/env bash

# ------------------------------------------------------------------------------
# This script connects to Cassandra via cqlsh and drops the 'ucsb_keyspace'.
# Make sure cqlsh is installed and accessible in your $PATH.
# Usage:
#   chmod +x drop_ucsb_keyspace.sh
#   ./drop_ucsb_keyspace.sh
# ------------------------------------------------------------------------------

KEYSPACE="ucsb_keyspace"
CASSANDRA_DATA_FOLER="/users/gaurangi/data_ssd/data"

# If Cassandra is running on a non-default host or port, specify with -u (host)
# and -p (port). For example:
#   HOST="1.2.3.4"
#   PORT=9042
#   cqlsh $HOST $PORT -e "DROP KEYSPACE IF EXISTS $KEYSPACE;"

echo "Dropping keyspace: $KEYSPACE"
cqlsh -e "DROP KEYSPACE IF EXISTS $KEYSPACE;"

if [ $? -eq 0 ]; then
  echo "Keyspace '$KEYSPACE' dropped successfully."
else
  echo "Failed to drop keyspace '$KEYSPACE'. Check the logs or cqlsh output for more details."
fi

#  By default, Cassandra takes a snapshot of the data before dropping a keyspace or a table, and the on-disk directory
#  structure can remain (with or without leftover files). Specifically, Cassandra has an auto_snapshot setting
#  (in cassandra.yaml) which is enabled by default. When you drop a keyspace/table, Cassandra automatically creates
#  a snapshot to protect you from accidental data loss.

# Run nodetool clearsnapshot to remove all snapshots from every keyspace and
# table (or use the -t option to remove a specific snapshot).

nodetool clearsnapshot

KEYSPACE_DATA_FOLDER=$CASSANDRA_DATA_FOLER/$KEYSPACE
echo "Deleting data folder for keyspace $KEYSPACE, data folder = $KEYSPACE_DATA_FOLDER" && rm -rfv $KEYSPACE_DATA_FOLDER