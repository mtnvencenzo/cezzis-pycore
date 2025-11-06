# 🍸 Contributing to Cezzis Cocktails RAG Agent

Thank you for your interest in contributing to the Cezzis Cocktails RAG Agent! This repository contains multiple applications working in tandem as a RAG (Retrieval-Augmented Generation) solution for the Cezzis.com website. We welcome contributions that help improve data ingestion, vector storage, query processing, and integration with the broader Cezzis.com ecosystem.

## 📋 Table of Contents

- [Getting Started](#-getting-started)
- [Development Setup](#-development-setup)
- [Contributing Process](#-contributing-process)
- [Code Standards](#-code-standards)
- [Testing](#-testing)
- [Deployment](#-deployment)
- [Getting Help](#-getting-help)

## 🚀 Getting Started

### 🧰 Prerequisites

Before you begin, ensure you have the following installed:
- Python 3.12+
- Make
- Docker & Docker Compose
- Terraform (optional, for IaC under `terraform/`)
- Git

### 🗂️ Project Structure

```text
cezzis-com-cocktails-rag-agent/
├── data-ingestion/
│   └── data-extraction-agent/    # Kafka consumer for cocktail data extraction
│       ├── src/                  # Python application code
│       ├── test/                 # Unit and integration tests
│       ├── Dockerfile            # Production container
│       ├── Dockerfile-CI         # CI/CD container
│       └── requirements.txt      # Python dependencies
├── terraform/                    # Infrastructure as Code (Azure)
└── .github/                      # GitHub workflows and templates
```

### 🎯 Applications Overview

This repository contains multiple interconnected services:
- **Data Extraction Agent**: Kafka consumer that processes cocktail data updates
- _(More services to be added as the RAG solution evolves)_

## 💻 Development Setup

1. **Fork and Clone the Repository**
   ```bash
   git clone https://github.com/mtnvencenzo/cezzis-com-cocktails-rag-agent.git
   cd cezzis-com-cocktails-rag-agent
   ```

2. **Set Up Data Extraction Agent**
   ```bash
   cd data-ingestion/data-extraction-agent
   
   # Create virtual environment
   python3 -m venv .venv
   source .venv/bin/activate
   
   # Install dependencies
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

3. **Run Tests**
   ```bash
   # Run unit tests
   make test
   
   # Run with coverage
   pytest --cov=. --cov-report=term
   ```

4. **Run Locally**
   ```bash
   # Set environment variables
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   export KAFKA_CONSUMER_GROUP=extraction-group
   export KAFKA_TOPIC_NAME=cocktails-topic
   
   # Run the application
   python src/app.py
   ```

5. **Docker Compose (Optional)**
   ```bash
   docker compose up
   ```

## 🔄 Contributing Process

### 1. 📝 Before You Start

- **Check for existing issues** to avoid duplicate work
- **Create or comment on an issue** to discuss your proposed changes
- **Wait for approval** from maintainers before starting work (required for this repository)

### 2. 🛠️ Making Changes

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

2. **Make your changes** following our [code standards](#-code-standards)

3. **Test your changes**
   ```bash
   make test
   ```

4. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat(extraction): add new functionality for ..."
   ```
   
   Use [conventional commit format](https://www.conventionalcommits.org/):
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `style:` for formatting changes
   - `refactor:` for code refactoring
   - `test:` for adding tests
   - `chore:` for maintenance tasks

### 3. 📬 Submitting Changes

1. **Push your branch**
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create a Pull Request**
   - Use our [PR template](pull_request_template.md)
   - Fill out all sections completely
   - Link related issues using `Closes #123` or `Fixes #456`
   - Request review from maintainers

## 📏 Code Standards

### 🐍 Python

- Follow PEP 8 style guidelines
- Use type hints for function signatures
- Write comprehensive docstrings
- Use structured logging
- Avoid global state; prefer dependency injection
- Keep functions small and focused

### 🧪 Code Quality

```bash
# Run tests
make test

# Lint and formatting with ruff
ruff format .
ruff check .
```

### 🌱 Infrastructure (Terraform)

- **Terraform**: Use Terraform best practices
- **Variables**: Define all variables in `variables.tf`
- **Documentation**: Document all resources and modules
- **State**: Never commit `.tfstate` files

## 🧪 Testing

### 🧪 Unit Tests
```bash
make test
```


### 📏 Test Requirements

- **Unit Tests**: All new features must include unit tests
- **E2E Tests**: Critical user flows should have E2E test coverage
- **Coverage**: Maintain minimum 80% code coverage
- **Test Naming**: Use descriptive test names that explain the behavior

## 🆘 Getting Help

### 📡 Communication Channels

- **Issues**: Use GitHub issues for bugs and feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Email**: Contact maintainers directly for sensitive issues

### 📄 Issue Templates

Use our issue chooser:
- https://github.com/mtnvencenzo/cezzis-com-cocktails-rag-agent/issues/new/choose

### ❓ Common Questions

**Q: How do I run the application locally?**
A: Follow the [Development Setup](#-development-setup) section above. Each application has its own setup instructions.

**Q: How do I run tests?**
A: Use `make test` in the respective application directory (e.g., `data-ingestion/data-extraction-agent/`).

**Q: Which application should I contribute to?**
A: Check the issue description - it should indicate which application is affected. If unsure, ask in the issue comments.

**Q: Can I contribute without approval?**
A: No, all contributors must be approved by maintainers before making changes.

**Q: How do I report a security vulnerability?**
A: Please email the maintainers directly rather than creating a public issue.

## 📜 License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project (see [LICENSE](../LICENSE)).

---

**Happy Contributing! 🍸**

For any questions about this contributing guide, please open an issue or contact the maintainers.
