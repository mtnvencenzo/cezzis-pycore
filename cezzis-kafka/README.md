# install poetry
curl -sSL https://install.python-poetry.org | python3 -
poetry --version

## navigate to project root and run:
poetry init

# dependencies
poetry add pydantic-settings
poetry add confluent-kafka
poetry add types-confluent_kafka

# dev dependencies
poetry add pytest --dev
peotry add pytest-cov --dev
poetry add pytest-mock --dev
poetry add ruff --dev
