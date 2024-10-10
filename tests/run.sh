pytest --strict-markers \
       --cov=alsek \
       --cov-report term-missing \
       --cov-fail-under 90 \
       --no-flaky-report \
       --showlocals \
       -vv
