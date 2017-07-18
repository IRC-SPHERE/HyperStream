#!/bin/bash

version=$(python -c 'import hyperstream; print(hyperstream.__version__)' 2>&1)

pandoc --from=markdown --to=rst --output=README README.md
python setup.py sdist bdist_wheel
twine upload dist/hyperstream-${version}.tar.gz
