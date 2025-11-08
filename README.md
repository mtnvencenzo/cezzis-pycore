# Cezzis PyCore

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)

A collection of production-ready Python packages for building distributed systems and data pipelines. Each package is independently versioned and published to PyPI.

## 📦 Packages

### [Cezzis Kafka](./kafka-packages/)

[![PyPI version](https://img.shields.io/pypi/v/cezzis-kafka.svg)](https://pypi.org/project/cezzis-kafka/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI/CD](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-kafka-cicd.yaml/badge.svg)](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-kafka-cicd.yaml)

A lightweight library for Apache Kafka message consumption with built-in error handling, multi-process support, and structured logging.

```bash
pip install cezzis-kafka
```

📦 [PyPI Project](https://pypi.org/project/cezzis-kafka/)  
📖 [Documentation](./kafka-packages/README.md)

---

### [Cezzis OTel](./otel-packages/)

[![PyPI version](https://img.shields.io/pypi/v/cezzis-otel.svg)](https://pypi.org/project/cezzis-otel/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI/CD](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-otel-cicd.yaml/badge.svg)](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-otel-cicd.yaml)

A lightweight library for initializing opentelemetry traces and logs for use with otlp exporters and otel collectors.

```bash
pip install cezzis-otel
```

📦 [PyPI Project](https://pypi.org/project/cezzis-otel/)  
📖 [Documentation](./otel-packages/README.md)

---

## 🚀 Getting Started

Each package in this repository is self-contained with its own:
- Dependencies and version management
- Documentation and examples
- Test suite
- CI/CD pipeline

Navigate to the individual package directories for detailed documentation and usage examples.

## 🛠️ Development

This repository uses a monorepo structure with independent package directories. Each package can be developed and released independently.

### Repository Structure

```
cezzis-pycore/
├── kafka-packages/          # Kafka consumer library
│   ├── cezzis_kafka/       # Package source code
│   ├── test/               # Package tests
│   ├── README.md           # Package documentation
│   └── pyproject.toml      # Package dependencies
├── otel-packages/           # Opentelemetry library
│   ├── cezzis_otel/       # Package source code
│   ├── test/               # Package tests
│   ├── README.md           # Package documentation
│   └── pyproject.toml      # Package dependencies
└── .github/                # CI/CD workflows and templates
```

### Contributing

We welcome contributions! Please see our [Contributing Guide](./.github/CONTRIBUTING.md) for details on:
- Code of Conduct
- Development workflow
- Pull request process
- Coding standards

### Opening Issues

- **Bug Reports:** Use the [bug report template](./.github/ISSUE_TEMPLATE/bug_report.md)
- **Feature Requests:** Use the [user story template](./.github/ISSUE_TEMPLATE/user-story-template.md)
- **Tasks:** Use the [task template](./.github/ISSUE_TEMPLATE/task-template.md)

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔒 Security

For security concerns, please review our [Security Policy](./.github/SECURITY.md).

## 💬 Support

- **Issues:** [GitHub Issues](https://github.com/mtnvencenzo/cezzis-pycore/issues)
- **Discussions:** [GitHub Discussions](https://github.com/mtnvencenzo/cezzis-pycore/discussions)
- **Email:** rvecchi@gmail.com

For package-specific questions, please refer to the individual package documentation.

---

**More packages coming soon!**
