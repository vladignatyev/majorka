#!/usr/bin/env bash
if $(command -v coverage); then
  coverage run -m framework.tests
else
  python -m framework.tests
fi
