# 🐍 Contributing to Cezzis PyCore

Thank you for your interest in contributing to Cezzis PyCore! This repository contains Python packages for distributed systems. We welcome contributions that help improve package functionality, performance, testing, and documentation.

## 📋 Table of Contents

- [Getting Started](#-getting-started)
- [Development Setup](#-development-setup)
- [Contributing Process](#-contributing-process)
- [Code Standards](#-code-standards)
- [Testing](#-testing)
- [Getting Help](#-getting-help)

## 🚀 Getting Started

### 🧰 Prerequisites

Before you begin, ensure you have the following installed:
- Python 3.12+
- Poetry (for dependency management)
- Make
- Docker & Docker Compose (for integration testing)
- Git

### 🗂️ Project Structure

```text
cezzis-pycore/
├── cezzis-oauth/               # Oauth related functionality
├── cezzis-oauth-fastapi/       # Oauth related functionality specifically for fastapi
├── cezzis-kafka/               # Kafka related functionality
├── cezzis-otel/                # OpenTelemetry related functionality
├── .github/                    # GitHub workflows and templates
└── README.md                   # Repository documentation
```

## 💻 Development Setup

1. **Fork and Clone the Repository**
   ```bash
   git clone https://github.com/mtnvencenzo/cezzis-pycore.git
   cd cezzis-pycore
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
   git commit -m "feat(kafka): add new functionality for ..."
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

### 📦 Package Management

- **Poetry**: Use Poetry for dependency management
- **pyproject.toml**: Define all dependencies and package metadata
- **Versioning**: Follow semantic versioning (semver)
- **Dependencies**: Keep dependencies minimal and well-documented

## 🧪 Testing

### 🧪 Unit Tests
```bash
make test
```


### 📏 Test Requirements

- **Unit Tests**: All new features must include unit tests
- **Coverage**: Maintain minimum 80% code coverage
- **Test Naming**: Use descriptive test names that explain the behavior
- **Mocking**: Use pytest-mock for external dependencies

## 🆘 Getting Help

### 📡 Communication Channels

- **Issues**: Use GitHub issues for bugs and feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Email**: Contact maintainers directly for sensitive issues

### 📄 Issue Templates

Use our issue chooser:
- https://github.com/mtnvencenzo/cezzis-pycore/issues/new/choose

### ❓ Common Questions

**Q: How do I set up the development environment?**
A: Follow the [Development Setup](#-development-setup) section above. Each package has its own directory with setup instructions.

**Q: How do I run tests?**
A: Use `make test` in the respective package directory (e.g., `cezzis-kafka/`).

**Q: Which package should I contribute to?**
A: Check the issue description - it should indicate which package is affected. If unsure, ask in the issue comments.

**Q: Can I contribute without approval?**
A: No, all contributors must be approved by maintainers before making changes.

**Q: How do I report a security vulnerability?**
A: Please email the maintainers directly rather than creating a public issue.

## 📜 License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project (see [LICENSE](../LICENSE)).

---

**Happy Contributing! 🍸**

For any questions about this contributing guide, please open an issue or contact the maintainers.
