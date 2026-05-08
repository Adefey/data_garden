pyupgrade --py314-plus $(find . -name "*.py" -type f)
autoflake --in-place --recursive .
isort . --line-length 120
black . --unstable --line-length 120
