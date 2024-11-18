pytest --strict-markers \
       --cov=alsek \
       --cov-report term-missing \
       --cov-fail-under 80 \
       --no-flaky-report \
       --showlocals \
       -vv
