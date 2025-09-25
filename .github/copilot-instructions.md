<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

# Grambo Project - Copilot Instructions

This is a workspace for the Grambo project cloned from https://github.com/claudionanni/grambo.

## Project Context
- **Repository**: claudionanni/grambo
- **Branch**: v2-rewrite (comprehensive entity architecture)
- **Project Type**: Advanced Galera cluster analysis platform
- **Languages**: Python (modern), Bash (legacy)
- **Purpose**: Comprehensive Galera cluster analysis with entity-based architecture
- **Setup Date**: September 15, 2025
- **Architecture**: Entity-based hierarchical cluster analysis system

## Project Structure
- `grambo`: Original bash script (legacy version)
- `gramboo.py`: Modern Python rewrite (transitional)
- `grap`: Next-generation entity-based parser (current focus)
- `graa`: Advanced cluster analyzer with comprehensive entity support
- `lib/entities/`: Comprehensive entity architecture framework
- `ref/entities/`: Entity research documentation and specifications
- `patterns/`: YAML-based pattern definitions for entity recognition
- `test_logs/`: Sample Galera log files for testing and validation
- `README.md`: Installation and basic usage instructions
- `COMPREHENSIVE_ENTITY_MODEL.md`: Complete entity architecture specification
- `ENTITY_IMPLEMENTATION_ROADMAP.md`: Development phases and priorities

## Usage
```bash
# Legacy bash version
./grambo galera-node-error.log
cat galera-node-error.log | ./grambo

# Modern Python version (recommended)
python3 gramboo.py galera-node-error.log
cat galera-node-error.log | python3 gramboo.py

# Python version with advanced options
./gramboo.py --format=json galera-node-error.log
./gramboo.py --filter=sst_event,state_transition galera-node-error.log
```

## Development Guidelines
- **Primary focus**: Python version (`gramboo.py`) for new features and improvements
- **Legacy support**: Maintain bash version (`grambo`) for backward compatibility
- Follow existing bash scripting patterns for the legacy version
- Follow Python best practices for the modern version
- Test changes thoroughly with sample Galera log files (`db3.log`)
- Ensure both versions remain executable and portable
- Document new features in the appropriate README files

## Python Version Features
- Organized event categorization (Server Info, Cluster Views, State Transitions, SST, IST, Communication Issues, Warnings, Errors)
- JSON output support for integration with other tools
- Event filtering by type
- Timeline analysis and summary statistics
- Better regex patterns and error handling
- Modular, maintainable code structure

## Extensions Installed
- ShellCheck: Static analysis for shell scripts
- Shell Format: Shell script formatter
