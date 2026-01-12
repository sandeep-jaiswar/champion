# Markdown Linting Configuration

## Overview

This project uses `markdownlint-cli` to validate markdown files. A `.markdownlintrc.json` configuration file has been created to customize linting rules.

## Configuration

The `.markdownlintrc.json` file disables the following rules:

- **MD022**: Headings should be surrounded by blank lines (disabled for documentation files)
- **MD024**: Multiple headings with same content (disabled for table structures)
- **MD025**: Single title/top-level heading per file (disabled for generated docs)
- **MD033**: Inline HTML (disabled for metadata tags and special formatting)
- **MD036**: Emphasis used instead of heading (disabled for intentional emphasis)
- **MD040**: Fenced code blocks should have language specified (disabled for legacy documentation)

## Running Linting

```bash
# Run with configuration
npx markdownlint-cli --config .markdownlintrc.json "**/*.md"

# Run with default rules (without config)
npx markdownlint-cli "**/*.md"
```

## CI/CD

The GitHub Actions workflow (`.github/workflows/markdownlint.yml`) automatically uses the configuration file.

## Rationale

The disabled rules were causing failures on legitimate documentation patterns:

1. **Generated documentation** has multiple sections with underline-style headers (MD022, MD025)
2. **API reference tables** have duplicate column headers like "Status" or "Tasks" (MD024)
3. **Custom HTML tags** are used for inline metadata and run IDs (MD033)
4. **Emphasis formatting** is intentional for status indicators (MD036)
5. **Legacy code blocks** in documentation lack language specifiers (MD040)

Rather than refactoring all documentation, we allow these patterns through configuration while maintaining code quality standards in active code files.

## Future Improvements

Consider gradually addressing these issues:

1. Add language specifiers to newly created code blocks
2. Refactor duplicate headers to use unique headings
3. Replace inline HTML with markdown alternatives where possible
4. Use proper heading levels instead of emphasis for structure

## References

- [markdownlint Configuration](https://github.com/igorshubovych/markdownlint-cli#configuration)
- [Markdown Lint Rules](https://github.com/markdownlint/markdownlint/blob/master/docs/Rules.md)
