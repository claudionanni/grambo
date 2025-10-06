# Changelog

All notable changes to Grambo will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### In Progress
- Flow control health status refinement based on actual cluster impact
- Additional temporal entity types for comprehensive cluster analysis

## [3.0.0-alpha.1] - 2025-01-06

### Added
- **v3 Tool Suite**: Complete rewrite with `graa3`, `grap3`, `graf3`, `grav3` tools
- **grax3 Orchestrator**: One-command pipeline execution wrapper
- **Flow Control Monitoring**: New entity type for tracking Galera flow control intervals
  - Flow control intervals parsing from logs
  - Timeline markers for flow control events
  - Dedicated flow control card in UI
  - Source code analysis integration for theoretical understanding
- **Natural Timeline**: Visual timeline bar showing actual time gaps between frames
  - Vertical markers for state change events
  - Clickable markers to jump to specific times
  - Differentiation between frame sequence and actual time progression
- **Enhanced UI**:
  - Draggable frame timeline with "drag to reposition" instruction
  - Documentation links in cards for contextual help
  - Improved card organization with proper sectioning
  - SST operation tracking with correct group assignment
  - View change counting improvements
- **Documentation System**:
  - In-card help links explaining metrics
  - Theory documentation for flow control
  - Frame navigation explanation
  - SST impact notes (joiner log reset warnings)

### Changed
- Renamed all tools with `3` suffix to indicate v3 generation:
  - `graa` → `graa3`
  - `grap` → `grap3`
  - `graf` → `graf3`
  - `grav` → `grav3`
  - `grax` → `grax3`
- Output file names updated to reflect tool versions (`graa3_sst.txt`)
- UI layout optimized for multiple analysis cards
- Section headers properly implemented for card organization
- Node data now correctly contained within cards

### Fixed
- **Critical**: SST operations counting in cluster details card
  - Now correctly sums SST operations per group
  - Assigns SST operations to correct group based on lifetime
  - Group lifetime defined as time from group first_seen to next group first_seen or now()
  - Fixed issue where all SST operations were assigned to first group
- **Critical**: View changes counting in cluster details card
  - Corrected excessive view change counts
  - Accurate per-group view change tracking
- SST Sessions viewer now points to correct file (`grax_output/sst_sessions.json`)
- SST Sessions link moved to more visible location in SST card
- Timeline markers now properly offset to avoid overlap
- Cards properly contained within their sections
- Node data display boundaries fixed

### Technical
- Entity extraction enhanced for flow control patterns
- Frame generation includes flow control state
- Group lifetime calculation for temporal entity assignment
- Improved entity-to-group correlation logic

## [2.0.0] - 2024 (v2-rewrite branch)

### Added
- Python rewrite of original bash script
- Entity-based architecture
- JSON/YAML output formats
- Pattern matching system
- SST/IST session tracking
- Web visualization with `grav`

### Changed
- Complete architectural redesign
- From line-based parsing to entity extraction
- Modular, maintainable code structure

## [1.0.0] - Original

### Added
- Original bash implementation (`grambo`)
- Basic Galera log parsing
- State transition tracking
- SST event detection
- Text-based output

---

## Version Naming Convention

- **v1.x**: Original bash implementation
- **v2.x**: Python rewrite with entity architecture (deprecated)
- **v3.x**: Next-generation tools with flow control, enhanced UI, and comprehensive analysis

## Links

- **Repository**: https://github.com/claudionanni/grambo
- **Issues**: https://github.com/claudionanni/grambo/issues
- **Releases**: https://github.com/claudionanni/grambo/releases

[Unreleased]: https://github.com/claudionanni/grambo/compare/v3.0.0-alpha.1...HEAD
[3.0.0-alpha.1]: https://github.com/claudionanni/grambo/releases/tag/v3.0.0-alpha.1
[2.0.0]: https://github.com/claudionanni/grambo/tree/v2-rewrite
[1.0.0]: https://github.com/claudionanni/grambo/tree/master
