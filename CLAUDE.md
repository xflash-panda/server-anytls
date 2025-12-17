# Project Guidelines

## Language

- All git commit messages must be written in English
- Code comments should be in English

## Git Commit Rules

- Do not include any AI-related information in commit messages
- Do not include "Co-Authored-By" or "Generated with" in commits
- Keep commit messages concise and descriptive
- Follow conventional commit format when appropriate

## Project Overview

This is a Go-based anytls server that provides proxy services for v2Board (XFLASH-PANDA).

## Build & Run

```bash
go build -o anytls-node ./cmd/server
```

## Code Style

- Follow standard Go conventions
- Use `gofmt` for formatting
- Handle errors explicitly
