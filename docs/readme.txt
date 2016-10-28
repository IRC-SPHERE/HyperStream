# Use sphinx-apidoc to build your API documentation:

sphinx-apidoc -f -o docs hyperstream

# Then execute the build

(cd docs && make html)