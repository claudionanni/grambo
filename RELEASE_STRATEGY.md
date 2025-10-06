# Grambo Release Strategy

This document outlines the recommended approach for managing releases, branches, and publishing alpha/beta versions to the public.

## Current Branch Structure

### Active Branches
- **`master`**: Stable, production-ready releases (legacy bash version)
- **`v2-alpha`**: Python rewrite (deprecated, superseded by v3)
- **`v2-rewrite`**: Comprehensive entity architecture (deprecated, superseded by v3)
- **`v3-alpha`**: Current development branch (next-generation tools with flow control, enhanced UI)

### Branch Purpose
| Branch | Purpose | Status | Target Users |
|--------|---------|--------|--------------|
| `master` | Stable bash version | Maintained | Production users |
| `v2-*` | Python prototypes | Deprecated | Historical reference |
| `v3-alpha` | Active development | Active | Early adopters, testers |

## Recommended Release Strategy

### Option 1: GitHub Releases with Pre-release Tags (Recommended)

This is the most common approach for open-source projects and provides clear versioning without branch complexity.

#### Strategy:
1. **Keep `master` as the stable branch** (current bash version stays here)
2. **Create pre-release tags from `v3-alpha`** to publish alpha/beta versions
3. **Merge `v3-alpha` into `master` when ready for GA** (General Availability)

#### Implementation:

```bash
# While on v3-alpha branch, create an alpha release
git checkout v3-alpha
git tag -a v3.0.0-alpha.1 -m "Grambo v3 Alpha 1 - Flow Control Support"
git push origin v3.0.0-alpha.1

# Create a GitHub Release from this tag
# Mark it as "Pre-release" in GitHub UI
```

#### Benefits:
- ✅ Clear versioning (semantic versioning with pre-release identifiers)
- ✅ Users can easily find and download specific versions
- ✅ `master` remains stable while v3 is tested
- ✅ GitHub shows pre-releases separately from stable releases
- ✅ Easy to track which version users are running
- ✅ Can publish release notes for each alpha/beta

#### Example Version Progression:
```
v3.0.0-alpha.1  → Initial alpha with basic v3 tools
v3.0.0-alpha.2  → Add flow control monitoring
v3.0.0-alpha.3  → UI improvements
v3.0.0-beta.1   → Feature complete, testing period
v3.0.0-beta.2   → Bug fixes
v3.0.0-rc.1     → Release candidate
v3.0.0          → General availability (merge to master)
```

### Option 2: Separate Beta Branch

Create a dedicated branch for beta testing.

#### Strategy:
```bash
# Create beta branch from v3-alpha
git checkout v3-alpha
git checkout -b v3-beta
git push -u origin v3-beta
```

#### Benefits:
- ✅ Separates experimental features from beta-quality code
- ✅ Users can track `v3-beta` branch for updates
- ❌ More complex branch management
- ❌ Less clear versioning

### Option 3: Make v3-alpha the Default Branch (Not Recommended Yet)

Point the repository's default branch to `v3-alpha`.

#### When to Consider:
- When v3 is feature-complete and stable enough for most users
- When you're ready to make v3 the primary offering
- When backward compatibility with v2/bash is no longer critical

#### Benefits:
- ✅ New clones get the latest version by default
- ❌ Can confuse existing users
- ❌ Loses stable default branch

## Recommended Approach for Grambo

Given your current situation, **I recommend Option 1** (GitHub Releases with Pre-release Tags):

### Phase 1: Alpha Releases (Current)
```bash
# Create alpha releases from v3-alpha branch
git checkout v3-alpha
git tag -a v3.0.0-alpha.1 -m "Grambo v3 Alpha 1 - Initial Release

Features:
- graa3/grap3/graf3/grav3 pipeline
- Flow control monitoring
- Enhanced UI with natural timeline
- SST session tracking
- Documentation links in cards"

git push origin v3.0.0-alpha.1
```

Then create a GitHub Release:
1. Go to https://github.com/claudionanni/grambo/releases/new
2. Choose tag: `v3.0.0-alpha.1`
3. Release title: "Grambo v3.0.0-alpha.1 - Flow Control Support"
4. ✅ Check "This is a pre-release"
5. Add release notes describing changes, known issues, and installation instructions
6. Attach any relevant binaries or archives (optional)

### Phase 2: Beta Releases (After Alpha Testing)
```bash
# Continue on v3-alpha branch
git tag -a v3.0.0-beta.1 -m "Grambo v3 Beta 1 - Feature Complete"
git push origin v3.0.0-beta.1
```

### Phase 3: Release Candidate (After Beta Testing)
```bash
git tag -a v3.0.0-rc.1 -m "Grambo v3 Release Candidate 1"
git push origin v3.0.0-rc.1
```

### Phase 4: General Availability (After RC Testing)
```bash
# Merge v3-alpha into master
git checkout master
git merge --no-ff v3-alpha -m "Merge v3 into master for GA release"
git tag -a v3.0.0 -m "Grambo v3.0.0 - General Availability"
git push origin master
git push origin v3.0.0
```

At this point:
- `master` contains the stable v3 release
- `v3-alpha` can continue for v3.1 development or be retired
- GitHub's default branch can optionally be changed to `master`

## Repository README Updates

### master Branch README (Current)
Keep it focused on the stable bash version with a prominent notice:

```markdown
# Grambo - Galera Cluster Log Analyzer

**⚠️ Note**: Grambo v3 is now available in alpha! Check out the [v3.0.0-alpha.1 release](https://github.com/claudionanni/grambo/releases/tag/v3.0.0-alpha.1) for the next-generation Python tools with advanced visualization.

## Stable Version (bash)
[... existing bash documentation ...]
```

### v3-alpha Branch README
Update it to indicate it's a pre-release:

```markdown
# Grambo v3 (Alpha) - Galera Cluster Log Analysis Suite

**⚠️ Alpha Version**: This is a pre-release version under active development. For production use, please use the [stable bash version](https://github.com/claudionanni/grambo).

[... v3 documentation ...]
```

## Communication Strategy

### For GitHub Users
1. **Release Page**: Use detailed release notes explaining what's new, what's changed, and what's been fixed
2. **Issues/Discussions**: Enable GitHub Discussions for alpha/beta feedback
3. **Labels**: Use issue labels like `v3-alpha`, `v3-beta` to track version-specific bugs

### For Documentation
1. Create a **CHANGELOG.md** tracking all versions
2. Maintain a **MIGRATION.md** guide for users moving from bash/v2 to v3
3. Keep **KNOWN_ISSUES.md** for each pre-release stage

### Example Release Notes Template

```markdown
# Grambo v3.0.0-alpha.1

## 🎉 New Features
- Complete rewrite with `graa3`, `grap3`, `graf3`, `grav3` pipeline
- Flow control monitoring and visualization
- Natural timeline with time gap visualization
- Interactive web UI with draggable frames
- SST session tracking with detailed reports

## 🔧 Improvements
- Better entity extraction accuracy
- Improved node state tracking
- Enhanced error handling

## 📚 Documentation
- Added inline help links in UI cards
- New flow control theory documentation
- Updated installation guide

## ⚠️ Known Issues
- Flow control health status needs refinement (#XX)
- Some edge cases in multi-node SST tracking (#XX)

## 🚀 Installation
```bash
git clone -b v3-alpha https://github.com/claudionanni/grambo.git
cd grambo
git checkout v3.0.0-alpha.1
# ... follow setup instructions ...
```

## 📝 Feedback
Please report issues at: https://github.com/claudionanni/grambo/issues
Tag your issues with the `v3-alpha` label.
```

## Semantic Versioning

Follow semantic versioning (semver) with pre-release identifiers:

```
v3.0.0-alpha.1
 │ │ │   │     │
 │ │ │   │     └─ Pre-release increment
 │ │ │   └─────── Pre-release stage (alpha, beta, rc)
 │ │ └─────────── PATCH (bug fixes)
 │ └───────────── MINOR (new features, backward compatible)
 └─────────────── MAJOR (breaking changes)
```

### When to Increment:
- **alpha.X**: Each significant development milestone
- **beta.X**: Each testing iteration after feature freeze
- **rc.X**: Each release candidate after bug fixes
- **Major (v3→v4)**: Breaking API/CLI changes
- **Minor (v3.0→v3.1)**: New features, backward compatible
- **Patch (v3.0.0→v3.0.1)**: Bug fixes only

## Best Practices

### 1. Changelog
Maintain a CHANGELOG.md following [Keep a Changelog](https://keepachangelog.com/):

```markdown
# Changelog

## [Unreleased]

## [3.0.0-alpha.1] - 2025-01-06
### Added
- Flow control monitoring
- Natural timeline visualization
- ...

### Changed
- Renamed tools to v3 suffix (graa → graa3, etc.)
- ...

### Fixed
- SST operation counting in cluster details card
- ...
```

### 2. Branch Protection
Once you're ready for beta:
- Protect `master` branch (require PR reviews)
- Protect `v3-beta` branch (require PR reviews)
- Keep `v3-alpha` open for rapid development

### 3. Testing
Before each release:
```bash
# Run tests
python3 -m pytest tests/

# Test full pipeline
./grax3 test_logs/sample.log

# Verify artifacts
ls -la grax_output/
```

### 4. Documentation
Before each release:
- Update version numbers in documentation
- Test all example commands
- Update screenshots if UI changed
- Review and update architecture docs

## Timeline Example

```
Now (v3-alpha branch):
  ├─ v3.0.0-alpha.1 (Jan 2025) ← Create this first
  ├─ v3.0.0-alpha.2 (Feb 2025) ← Bug fixes + refinements
  ├─ v3.0.0-alpha.3 (Mar 2025) ← More testing feedback
  ├─ v3.0.0-beta.1  (Apr 2025) ← Feature freeze
  ├─ v3.0.0-beta.2  (May 2025) ← Bug fixes only
  ├─ v3.0.0-rc.1    (Jun 2025) ← Release candidate
  └─ v3.0.0 (Jul 2025) ← Merge to master, GA release
```

## Conclusion

**Recommended Next Steps:**

1. ✅ Create `v3.0.0-alpha.1` tag from current `v3-alpha` branch
2. ✅ Create GitHub Release (mark as pre-release)
3. ✅ Update README on `v3-alpha` to indicate alpha status
4. ✅ Add notice to `master` README about v3 alpha availability
5. ✅ Create CHANGELOG.md
6. ✅ Solicit feedback from early adopters
7. ⏭️ Iterate with alpha.2, alpha.3, etc. based on feedback
8. ⏭️ Move to beta when feature-complete and stable
9. ⏭️ Merge to master when ready for production use

This approach provides a clear path for users to find and test the alpha version while keeping the stable version easily accessible.
