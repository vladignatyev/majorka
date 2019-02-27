#!/usr/bin/env bash
if $(test coverage); then
  coverage erase && coverage run --branch --source framework/ -m framework.tests
  coverage report -m --skip-covered
else
  python -m framework.tests
fi
