: << 'END_DOCSTRING'
  recovery.sh
  Run the recovery test, which looks something like the following.
  First, we run the `fail` process and wait for it to finish creating the database and commit. When `fail` has finished,
  it will write all of the record values to a file, in order. The keys for one of these databases always consists of
  sequential integers from 0 to N, exclusive, where N is the number of records in the database. Next we wait, allowing
  the process to modify the database at random for a while. This lets some of the database contents get overwritten.
  We then SIGKILL the `fail` process. Since the database was in the middle of a transaction when the process died, it
  will be left in an invalid state. To remedy this, we run the `recover` process. `recover` will reopen the database
  and perform recovery. It will then validate the database contents against the N values written out by `fail`. Either
  0 or 1 will be returned from this script to indicate success or failure, respectively.
END_DOCSTRING

PASS_LABEL="[$(tput setaf 2)PASS$(tput sgr0)]"
FAIL_LABEL="[$(tput setaf 1)FAIL$(tput sgr0)]"
TOPLEVEL_PREFIX='> recovery.sh:'
INSTANCE_PREFIX='> '
DONE_PREFIX=/tmp/calico_recovery_done_
FILE_PREFIX=/tmp/calico_recovery_database_
TEMP_PREFIX=/tmp/calico_recovery_out_
OUTPUT_SINK=/dev/null

NUM_INSTANCES=10
INSTANCE_SIZE=10000
SEED=0
WAIT_TIME=1
VERBOSE=0

while getopts n:r:s:vw: FLAG; do
  case "${FLAG}" in
    n) NUM_INSTANCES=${OPTARG};;
    r) INSTANCE_SIZE=${OPTARG};;
    s) SEED=${OPTARG};;
    v) VERBOSE=1;;
    w) WAIT_TIME=${OPTARG};;
    *) echo Status: Invalid option "${FLAG}";;
  esac
done

function outer_echo() {
  if [ "$VERBOSE" -eq 1 ]; then
    echo "$TOPLEVEL_PREFIX $1"
  fi
}

function inner_echo() {
  if [ "$VERBOSE" -eq 1 ]; then
    printf "%s%3d: %s\n" "$INSTANCE_PREFIX" "$1" "$2"
  fi
}

function run_instance() {
  local I="$1"
  local DONE_PATH="$DONE_PREFIX$I"
  local FILE_PATH="$FILE_PREFIX$I"
  local TEMP_PATH="$TEMP_PREFIX$I"
  local SIZE="$2"
  local WAIT="$3"
  local SEED="$4"

  rm -rf "$FILE_PATH"
  rm -f "$TEMP_PATH"
  true > "$DONE_PATH"

  # `fail` will create a database with known keys, commit, then modify at random.
  inner_echo "$I" "Building database..."
  ./fail "$FILE_PATH" "$SIZE" "$SEED" > "$DONE_PATH" 2>&1 &
  local PID="$!"

  # Wait for `fail` to finish creating the database.
  while [ ! -s "$DONE_PATH" ]; do
    sleep 1
  done
  inner_echo "$I" "Database is at $FILE_PATH"
  rm -f "$DONE_PATH"

  # Let `fail` modify the database for a bit.
  inner_echo "$I" "Modifying database..."
  sleep "$WAIT"

  # Kill `fail`, leaving an uncommitted database with a WAL.
  inner_echo "$I" "Killing process..."
  kill -9 "$PID"
  wait "$PID" 2>"$OUTPUT_SINK"

  # `recover` will restore the database from the WAL and run some basic validation.
  inner_echo "$I" "Running recovery..."
  ./recover "$FILE_PATH" "$SIZE" > "$TEMP_PATH" 2>&1
}

outer_echo "Running $A_BOLD$NUM_INSTANCES$A_OFF instances of size $A_BOLD$INSTANCE_SIZE$A_OFF"
outer_echo "Each instance will be killed $WAIT_TIME second(s) after it commits"

for (( I=0; I<NUM_INSTANCES; I++ )); do
  run_instance "$I" "$INSTANCE_SIZE" "$WAIT_TIME" "$SEED" &
done

wait

RC=0
for (( I=0; I<NUM_INSTANCES; I++ )); do
  TEMP_PATH="$TEMP_PREFIX$I"

  # When an instance fails, it will write to stderr, which is redirected to this storage. If it passes, it won't
  # output anything.
  if [ -s "$TEMP_PATH" ]; then
    inner_echo "$I" "$FAIL_LABEL"
    cat < "$TEMP_PATH" | sed 's/^/  /'
    RC=1
  else
    inner_echo "$I" "$PASS_LABEL"
    rm -f "$TEMP_PATH"
  fi
done

outer_echo "Finished"
if [ "$RC" -eq 0 ]; then
  outer_echo "$PASS_LABEL"
else
  outer_echo "$FAIL_LABEL"
fi
exit "$RC"