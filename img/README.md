# Images Directory

This directory contains reference images used in the Grambo project documentation.

## Purpose
- Screenshots of tool outputs for documentation
- Diagrams illustrating Galera cluster concepts
- Visual examples of SST/IST workflows
- Architecture diagrams for the entity model

## File Organization
- Use descriptive filenames with prefixes:
  - `sst_` - SST (State Snapshot Transfer) related images
  - `ist_` - IST (Incremental State Transfer) related images
  - `graa_` - graa tool output examples
  - `grap_` - grap tool output examples
  - `arch_` - Architecture diagrams
  - `flow_` - Workflow diagrams

## Supported Formats
- PNG (preferred for screenshots)
- JPG/JPEG (for photos)
- SVG (for diagrams and vector graphics)
- GIF (for animated demonstrations)

## Current Images
- `graa_sst_ist_tree_example.png` - Terminal screenshot showing graa --sst-ist-tree output with hierarchical SST+IST relationships and progress indicators
- `graa_sst_ist_no_backup_transfer.png` - SST+IST tree showing scenario where SST occurs without actual backup transfer (script only), followed by extensive IST processing

## Usage in Documentation
Reference images in markdown files using relative paths:
```markdown
![SST+IST Tree Example](../img/graa_sst_ist_tree_example.png)
```

## Guidelines
- Keep image files reasonably sized (< 1MB when possible)
- Use clear, descriptive alt text
- Include brief descriptions in commit messages when adding images