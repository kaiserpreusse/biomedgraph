#!/usr/bin/env bash
pip install -r test_requirements.txt
pip install -r requirements.txt
pip install -e .
pytest