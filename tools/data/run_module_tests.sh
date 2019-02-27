#!/usr/bin/env bash
if $(command -v coverage); then
  coverage erase && coverage run --branch --source . test.py
  coverage report -m --skip-covered
else
  python -m test.py
fi
