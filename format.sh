pyupgrade --py314-plus $(find . -name "*.py" -type f)
autoflake --in-place --recursive --remove-all-unused-imports --expand-star-imports --remove-duplicate-keys .
isort . --line-length 100
black . --unstable --line-length 100
prettier $(find . -name "*.html" -type f) --write
