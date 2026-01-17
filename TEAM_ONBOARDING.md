# 📋 Team Onboarding Checklist

## For New Team Members

### Day 1: Orientation

- [ ] **Read Documentation** (1 hour)
  - [ ] Start with [VISUAL_GUIDE.md](VISUAL_GUIDE.md) (20 min)
  - [ ] Read [ARCHITECTURE.md](docs/ARCHITECTURE.md) (20 min)
  - [ ] Skim [QUICK_REFERENCE.md](QUICK_REFERENCE.md) (10 min)

- [ ] **Set Up Environment** (30 min)
  - [ ] Clone repository: `git clone ...`
  - [ ] Install dependencies: `poetry install`
  - [ ] Copy `.env`: `cp .env.example .env` (or ask team for values)
  - [ ] Run tests: `poetry run pytest tests/ -v`
  - [ ] Verify setup: `poetry run champion show-config`

- [ ] **Explore Codebase** (30 min)
  - [ ] Navigate `src/champion/` directory structure
  - [ ] Open `src/champion/__init__.py` - see public API
  - [ ] Open `src/champion/core/__init__.py` - understand core exports
  - [ ] Check 2-3 domain modules (`scrapers`, `storage`, `warehouse`)

- [ ] **Try a Command** (15 min)
  - [ ] `poetry run champion --help`
  - [ ] `poetry run champion show-config` - see configuration
  - [ ] Try one ETL command (ask team which is safe)

### Day 2-3: Deep Dive

- [ ] **Understanding Core Module** (2 hours)
  - [ ] Read [docs/ARCHITECTURE.md - Core Module section](docs/ARCHITECTURE.md#core-module-champion-core)
  - [ ] Look at `src/champion/core/interfaces.py` - understand contracts
  - [ ] Look at `src/champion/core/errors.py` - understand error types
  - [ ] Look at `src/champion/core/di.py` - understand DI pattern
  - [ ] Look at `src/champion/core/config.py` - understand configuration

- [ ] **Understanding Domains** (2 hours)
  - [ ] Read one domain section from ARCHITECTURE.md
  - [ ] Look at domain `__init__.py` files
  - [ ] Look at domain `adapters.py` files
  - [ ] Trace data flow through domain

- [ ] **Understanding Your Assignment** (1 hour)
  - [ ] Identify which domain(s) you'll work on
  - [ ] Read that domain's ARCHITECTURE section carefully
  - [ ] Look at test examples in `tests/` for patterns
  - [ ] Ask questions about design decisions

- [ ] **Ask Questions** (ongoing)
  - [ ] What problem does this pattern solve?
  - [ ] How does data flow through this?
  - [ ] Why is it designed this way?
  - [ ] What should I avoid?

---

## For Existing Team Members

### Migration Preparation

- [ ] **Assess Current Code** (2 hours)
  - [ ] List all custom scrapers
  - [ ] List all storage operations
  - [ ] List all warehouse operations
  - [ ] Identify dependencies between modules
  - [ ] Document hard-coded configuration

- [ ] **Follow MIGRATION.md** (ongoing)
  - [ ] Read [docs/MIGRATION.md](docs/MIGRATION.md) completely
  - [ ] Start with Phase 1: Understanding (1 hour)
  - [ ] Move to Phase 2: Update Imports (2 hours per module)
  - [ ] Follow Phases 3-5: Implement new patterns

- [ ] **Update Import Statements** (per module)
  - [ ] Replace `from champion.config import config`
  - [ ] Replace `from champion.orchestration.config import Config`
  - [ ] Replace direct logger imports
  - [ ] Replace warehouse/loader imports

- [ ] **Implement Dependency Injection** (per module)
  - [ ] Identify hard-coded dependencies
  - [ ] Change to constructor injection
  - [ ] Use interfaces (DataSink, DataSource, etc.)
  - [ ] Add to DI container if using one

- [ ] **Update Error Handling** (per module)
  - [ ] Replace generic `Exception` catching
  - [ ] Catch specific domain errors
  - [ ] Use `recovery_hint` for user guidance

- [ ] **Add Tests** (per module)
  - [ ] Create unit tests with mocks
  - [ ] Use fixtures from `tests/conftest.py`
  - [ ] Test error conditions
  - [ ] Test integration points

---

## For Architects/Tech Leads

### Governance

- [ ] **Establish Guidelines** (1 day)
  - [ ] Code review checklist for new code
  - [ ] Require interfaces for new abstractions
  - [ ] Require dependency injection
  - [ ] Require structured logging
  - [ ] Require custom error types

- [ ] **Architecture Review Process** (ongoing)
  - [ ] Review new modules follow patterns
  - [ ] Verify no circular dependencies
  - [ ] Check for tight coupling
  - [ ] Validate error handling
  - [ ] Review logging quality

- [ ] **Monitoring & Observability** (1 week)
  - [ ] Set up structured log analysis
  - [ ] Create alerts for error types
  - [ ] Monitor ETL flows
  - [ ] Track performance metrics

- [ ] **Documentation** (ongoing)
  - [ ] Keep [ARCHITECTURE.md](docs/ARCHITECTURE.md) current
  - [ ] Update [MIGRATION.md](docs/MIGRATION.md) for new patterns
  - [ ] Add domain-specific READMEs
  - [ ] Document new interfaces

---

## For DevOps/Platform Team

### Deployment

- [ ] **Update CI/CD** (1 day)
  - [ ] Update build steps for new structure
  - [ ] Verify all tests run
  - [ ] Update Docker build if used
  - [ ] Verify CLI entry points work

- [ ] **Configuration Management** (2 days)
  - [ ] Set environment-specific `.env` files
  - [ ] Update secrets management
  - [ ] Document configuration hierarchy
  - [ ] Create runbooks for common tasks

- [ ] **Monitoring** (2 days)
  - [ ] Parse structured JSON logs
  - [ ] Set up metrics collection
  - [ ] Create dashboards for key flows
  - [ ] Set up alerting

- [ ] **Documentation** (1 day)
  - [ ] Update deployment guides
  - [ ] Document environment variables
  - [ ] Update troubleshooting guide
  - [ ] Document backup/recovery

---

## Weekly Checklist

Every week, the team should:

- [ ] **Stand-up Updates**
  - Progress on modules being migrated
  - Blockers or questions
  - Design discussions needed

- [ ] **Code Review Focus**
  - Using correct error types? ✓
  - Using dependency injection? ✓
  - Using structured logging? ✓
  - Following interface contracts? ✓

- [ ] **Documentation Updates**
  - New patterns documented? ✓
  - Examples added to tests? ✓
  - Questions answered? ✓

- [ ] **Architecture Health**
  - No circular imports? ✓
  - No tight coupling? ✓
  - Tests passing? ✓
  - Coverage maintained? ✓

---

## Quarterly Goals

### Q1
- [ ] All scrapers use EquityScraper interface
- [ ] All storage uses DataSource/DataSink
- [ ] All error handling uses custom exceptions
- [ ] 90% test coverage

### Q2
- [ ] All workflows use DI
- [ ] Structured logging everywhere
- [ ] No hard-coded configuration
- [ ] Team training complete

### Q3
- [ ] Feature plugin system
- [ ] Multi-warehouse support
- [ ] Streaming integration
- [ ] GraphQL API

### Q4
- [ ] ML pipeline integration
- [ ] Advanced analytics features
- [ ] Performance optimization
- [ ] Documentation complete

---

## Common Questions

### Q: How do I add a new scraper?
**A:** See [MIGRATION.md - Scenario 1](docs/MIGRATION.md#scenario-1-adding-a-new-data-source)

### Q: How do I use the warehouse?
**A:** Check [QUICK_REFERENCE.md - Pattern 1](QUICK_REFERENCE.md#pattern-1-simple-data-processing)

### Q: How do I test my code?
**A:** See [QUICK_REFERENCE.md - Debugging](#debugging-tips) and examples in `tests/`

### Q: How do I configure for production?
**A:** See [ARCHITECTURE.md - Configuration Management](docs/ARCHITECTURE.md#configuration-management)

### Q: What's the error hierarchy?
**A:** See [QUICK_REFERENCE.md](QUICK_REFERENCE.md#core-module-exports) and `core/errors.py`

### Q: How do I add a new warehouse backend?
**A:** See [MIGRATION.md - Scenario 3](docs/MIGRATION.md#scenario-3-swapping-storage-backend)

---

## Troubleshooting

### Problem: Tests fail after setup
**Solution:**
1. Verify Python version: `python --version` (should be 3.11+)
2. Verify Poetry: `poetry --version`
3. Reinstall: `poetry install --no-cache`
4. Run specific test: `poetry run pytest tests/unit/test_core.py -v`

### Problem: Import errors
**Solution:**
1. Check import path in [QUICK_REFERENCE.md](QUICK_REFERENCE.md#core-module-exports)
2. Verify file exists in that location
3. Check `__init__.py` exports
4. Try: `python -c "import champion; print(champion.__version__)"`

### Problem: Configuration not loading
**Solution:**
1. Check `.env` file exists
2. Verify `poetry run champion show-config` works
3. Check environment variables: `env | grep CHAMPION`
4. Debug: `poetry run python -c "from champion.core import get_config; print(get_config())"`

### Problem: Don't understand a pattern
**Solution:**
1. Check test examples in `tests/`
2. Read relevant section in [ARCHITECTURE.md](docs/ARCHITECTURE.md)
3. Check code examples in [QUICK_REFERENCE.md](QUICK_REFERENCE.md#common-patterns)
4. Ask in team Slack/meeting

---

## Resources

### Documentation
- 📖 [ARCHITECTURE.md](docs/ARCHITECTURE.md) - Complete reference
- 🔄 [MIGRATION.md](docs/MIGRATION.md) - How to migrate code
- 🔧 [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Cheat sheet
- 📊 [VISUAL_GUIDE.md](VISUAL_GUIDE.md) - Before/after diagrams
- ✨ [ARCHITECTURE_TRANSFORMATION.md](ARCHITECTURE_TRANSFORMATION.md) - What was done

### Code Examples
- `tests/` - Unit and integration tests
- `src/champion/` - Clean code implementations
- Domain `adapters.py` - Pattern examples

### External Resources
- [Dependency Injection Pattern](https://en.wikipedia.org/wiki/Dependency_injection)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
- [SOLID Principles](https://en.wikipedia.org/wiki/SOLID)
- [Polars Documentation](https://pola-rs.github.io/polars/)
- [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/models/#settingsbasemodel)

---

## Getting Help

### Team Resources
- **Architecture Questions**: Ask tech lead
- **Code Questions**: Code review team member
- **Configuration Issues**: DevOps team
- **Test Help**: Look at similar test in `tests/`

### Documentation First
1. Check ARCHITECTURE.md section
2. Check QUICK_REFERENCE.md
3. Check test examples
4. Check docstrings in code
5. Ask team

---

## Success Criteria

You're ready when you can:

- ✅ Explain the 5 main layers of Champion
- ✅ Write code that uses dependency injection
- ✅ Implement a new adapter for a domain
- ✅ Write tests using mocks
- ✅ Use structured logging correctly
- ✅ Handle errors with appropriate types
- ✅ Add a new scraper/storage/warehouse backend
- ✅ Guide a new team member through setup
- ✅ Review code for architectural issues
- ✅ Propose improvements to the architecture

---

**Welcome to the Team!** 🎉

*Let's build great software together.*

---

*Last Updated: January 17, 2026*
*Version: 1.0*
