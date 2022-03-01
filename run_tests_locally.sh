#!/bin/sh
echo "Starting Flake8 test..."
flake8 --ignore E501 dags --benchmark || exit 1
echo "Starting Black test..."
python3 -m pytest --cache-clear
python3 -m pytest dags/ --black -v || exit 1
echo "Starting Pytest tests..."
cd tests || exit
python3 -m pytest tests.py -v || exit 1
echo "All tests completed successfully! 🥳"