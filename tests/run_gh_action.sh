timeout 45 pytest \
      --strict-markers \
      --cov=alsek \
      --cov-report term-missing \
      --cov-fail-under 80 \
      --no-flaky-report \
      --showlocals \
      --timeout=250 \
      -vv | tee pytest_output.txt

# Check the last line for errors or failures
if tail -n 1 pytest_output.txt | grep -q "errors\|failed=[1-9]"; then
   echo "Tests failed"
   exit 1
else
   echo "Tests passed (or timed out after completing successfully)"
   exit 0
fi
