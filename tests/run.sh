pytest tests/core/worker/test_process.py \
       --strict-markers \
       --cov=alsek \
       --cov-report term-missing \
       --cov-fail-under 80 \
       --no-flaky-report \
       --showlocals \
       --timeout=250 \
       -vv
