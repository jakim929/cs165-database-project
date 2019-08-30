# sample usage:
#       ./gen_all_for_staff_use.sh ~/repo/cs165-docker-test-runner/test_data

# TODO: somebody needs to add here and pick some good configs for correctness tests
# especially for milestone4 and beyond.

OUTPUT_TEST_DIR=$1
DOCKER_TEST_DIR="${2:-/cs165/staff_test}"
TBL_SIZE="${3:-10000}"
RAND_SEED="${4:-42}"

echo "FILES WILL BE WRITTEN TO: $OUTPUT_TEST_DIR"
echo "DSL SCRIPTS WILL LOAD FROM THIS DOCKER DIR: $DOCKER_TEST_DIR"
echo "DSL SCRIPTS WILL USE DATA SIZE: $TBL_SIZE"
echo "DSL SCRIPTS WILL USE RANDOM SEED: $RAND_SEED"

echo "DATA GENERATION STEP BEGIN ..."

python milestone1.py $TBL_SIZE $RAND_SEED ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}
python milestone2.py $TBL_SIZE $RAND_SEED ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}
python milestone3.py $TBL_SIZE $RAND_SEED ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}

python milestone4.py $TBL_SIZE 10000 10000 $RAND_SEED 1.0 50 ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}
python milestone5.py $TBL_SIZE $RAND_SEED ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}

echo "DATA GENERATION STEP FINISHED ..."