pytest --strict-markers \
       --cov=alsek \
       --cov-report term-missing \
       --cov-fail-under 80 \
       --no-flaky-report \
       --showlocals \
       --timeout=250 \
       -vv

# Capture exit code
EXIT_CODE=$?

# If tests pass (exit code 0), then force exit
if [ $EXIT_CODE -eq 0 ]; then
    echo "Tests passed successfully, forcing clean exit"
    # Use kill -9 on the current process group
    kill -9 0
else
    # Otherwise exit with pytest's exit code
    exit $EXIT_CODE
fi
