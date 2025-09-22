# Grambo Refactoring Technical Specification v1.0

**Document Purpose**: Single source of truth for all technical decisions in the grambo entity-based refactoring.
**Last Updated**: September 22, 2025
**Status**: DRAFT - Implementation Phase

---

## 1. PROJECT OVERVIEW

### 1.1 Current State
- **gra.py**: Monolithic log parser + analyzer (1200+ lines)
- **gras.py**: Frame-based state machine builder
- **graw.py**: Web visualization tool
- **Problem**: Brittle regex patterns break with new MariaDB versions, semantic relationships lost

### 1.2 Target State
- **grap.py**: Dedicated entity-based log parser with interactive learning
- **gra.py**: Refactored to consume structured data from grap
- **gras.py**: Minimal changes, works with structured input
- **graw.py**: No changes required

### 1.3 Core Innovation
**Paradigm Shift**: Pattern Matching → Entity Extraction + Relationship Mapping

---

## 2. ARCHITECTURE DECISIONS

### 2.1 Entity-Based Design

#### 2.1.1 Core Entities (Phase 1)
```yaml
MANDATORY_ENTITIES:
  - NODE: Cluster node representation
  - STATE_TRANSFER: SST/IST operations  
  - VIEW: Cluster view changes

FUTURE_ENTITIES:
  - CONFLICT: Certification conflicts
  - FLOW_CONTROL: Network flow control events
  - ERROR: Error conditions and failures
```

#### 2.1.2 Entity Structure
```python
@dataclass
class Entity:
    entity_id: str           # Unique identifier: "{type}_{timestamp}_{context}"
    entity_type: EntityType  # Enum of supported types
    timestamp: datetime      # When entity was created/observed
    properties: Dict[str, Any]  # Type-specific properties
    relationships: List[Relationship]  # Links to other entities
    confidence: float        # Parsing confidence (0.0-1.0)
    source_lines: List[int]  # Original log line numbers
    metadata: Dict[str, Any] # Parser metadata
```

#### 2.1.3 Unique ID Strategy
```python
ENTITY_ID_PATTERNS = {
    "NODE": "node_{timestamp}_{name}_{uuid_short}",
    "STATE_TRANSFER": "sst_{timestamp}_{donor}_{joiner}",
    "VIEW": "view_{timestamp}_{view_id}",
    "CONFLICT": "conflict_{timestamp}_{seqno}",
    "FLOW_CONTROL": "fc_{timestamp}_{node}_{type}",
    "ERROR": "error_{timestamp}_{node}_{type}"
}
```

### 2.2 Interactive Learning System

#### 2.2.1 Confidence Thresholds
```python
CONFIDENCE_LEVELS = {
    "HIGH": 0.9,      # Auto-accept, no user interaction
    "MEDIUM": 0.7,    # Suggest pattern, ask for confirmation
    "LOW": 0.4,       # Present options, ask for guidance
    "UNKNOWN": 0.0    # Full interactive mode
}
```

#### 2.2.2 Learning Flow
1. **Pattern Matching**: Try existing patterns
2. **Confidence Check**: Evaluate match quality
3. **User Interaction**: Ask for guidance if confidence < 0.7
4. **Pattern Learning**: Update pattern registry
5. **Entity Creation**: Generate entity with relationships

#### 2.2.3 User Interaction Modes
```python
INTERACTION_MODES = {
    "SILENT": "No user interaction, use defaults",
    "CONFIRM": "Ask confirmation for uncertain patterns",
    "TEACH": "Full teaching mode with pattern explanation",
    "BATCH": "Collect uncertainties, ask in batches"
}
```

### 2.3 Pattern Registry Architecture

#### 2.3.1 Pattern Storage
```yaml
# ~/.grambo/patterns/
patterns/
├── core/                    # Built-in patterns
│   ├── mariadb_10.4.yml
│   ├── mariadb_10.5.yml
│   └── mariadb_11.0.yml
├── learned/                 # User-taught patterns
│   ├── custom_patterns.yml
│   └── site_specific.yml
└── registry.json          # Pattern metadata and versioning
```

#### 2.3.2 Pattern Definition Format
```yaml
# Pattern example
pattern_id: "sst_request_v1"
entity_type: "STATE_TRANSFER"
mariadb_versions: ["10.4", "10.5", "10.6"]
confidence: 0.9
regex: "WSREP: Member (?P<joiner>\\S+) \\(\\S+\\) requested state transfer from '(?P<donor>\\S+)'"
properties:
  type: "request"
  joiner: "{joiner}"
  donor: "{donor}"
relationships:
  - type: "involves"
    target_entity: "NODE"
    target_property: "joiner"
examples:
  - "2024-01-15 10:23:45 0 [Note] WSREP: Member 0.4 (node-l05) requested state transfer from '*any*'"
```

### 2.4 Output Format Specification

#### 2.4.1 Structured Log Format
```json
{
  "grambo_version": "2.0.0",
  "parser_version": "1.0.0",
  "source_file": "/path/to/galera.log",
  "parsing_metadata": {
    "total_lines": 15432,
    "parsed_lines": 14890,
    "skipped_lines": 542,
    "confidence_distribution": {
      "high": 12456,
      "medium": 2334,
      "low": 100
    },
    "parsing_duration_ms": 2340,
    "patterns_used": ["sst_request_v1", "view_change_v2", ...]
  },
  "entities": [
    {
      "entity_id": "node_2024-01-15T10:23:45_l05_abc123",
      "entity_type": "NODE",
      "timestamp": "2024-01-15T10:23:45.123Z",
      "properties": {
        "name": "vinfr-db-d-l05",
        "uuid": "abc123-def456-...",
        "address": "10.220.26.6:4567",
        "initial_state": "CLOSED"
      },
      "relationships": [
        {
          "type": "participates_in",
          "target_entity": "sst_2024-01-15T10:23:45_d01_l05",
          "role": "joiner"
        }
      ],
      "confidence": 0.95,
      "source_lines": [1, 23, 45],
      "metadata": {
        "pattern_id": "node_definition_v1",
        "extraction_method": "regex"
      }
    }
  ],
  "relationships": [
    {
      "source_entity": "sst_2024-01-15T10:23:45_d01_l05",
      "target_entity": "view_2024-01-15T10:23:44_primary",
      "relationship_type": "triggered_by",
      "confidence": 0.8,
      "metadata": {
        "inference_method": "temporal_proximity",
        "time_delta_ms": 1200
      }
    }
  ],
  "timeline": [
    {
      "timestamp": "2024-01-15T10:23:45.000Z",
      "events": [
        "view_2024-01-15T10:23:44_primary",
        "sst_2024-01-15T10:23:45_d01_l05"
      ]
    }
  ]
}
```

---

## 3. IMPLEMENTATION PHASES

### 3.1 Phase 1: Core Parser Infrastructure (Week 1-2)
**Deliverables**:
- [ ] Basic `grap.py` command-line tool
- [ ] Entity base classes and data structures
- [ ] Simple pattern registry system
- [ ] 3 core entities: NODE, STATE_TRANSFER, VIEW
- [ ] Basic regex patterns for MariaDB 10.4/10.5/11.0
- [ ] JSON output compatible with current `gras` input expectations

**Success Criteria**:
- Parse sample logs without errors
- Generate valid JSON output
- Extract basic entities with >80% accuracy

### 3.2 Phase 2: Interactive Learning (Week 3-4)
**Deliverables**:
- [ ] Confidence scoring system
- [ ] Interactive CLI prompts
- [ ] Pattern learning and storage
- [ ] User feedback integration
- [ ] Batch processing mode

**Success Criteria**:
- Handle unknown patterns gracefully
- Learn new patterns from user input
- Maintain pattern registry persistence

### 3.3 Phase 3: Relationship Inference (Week 5-6)
**Deliverables**:
- [ ] Temporal relationship detection
- [ ] Causal relationship inference
- [ ] Entity cross-referencing
- [ ] Advanced entity properties

**Success Criteria**:
- Detect SST→View relationships
- Infer node participation in state transfers
- Generate relationship graphs

### 3.4 Phase 4: Integration & Optimization (Week 7-8)
**Deliverables**:
- [ ] Refactor `gra.py` to use structured input
- [ ] Performance optimization
- [ ] Comprehensive testing
- [ ] Documentation update

**Success Criteria**:
- `gras` works with new structured input
- Performance matches or exceeds current implementation
- Full backward compatibility

---

## 4. TECHNICAL SPECIFICATIONS

### 4.1 Dependencies
```python
# requirements.txt additions
pydantic>=2.0.0          # Data validation and serialization
orjson>=3.9.0            # Fast JSON processing
click>=8.0.0             # CLI interface
rich>=13.0.0             # Beautiful terminal output
questionary>=1.10.0      # Interactive prompts
jinja2>=3.1.0            # Pattern templating
pyyaml>=6.0              # Pattern file format
```

### 4.2 File Structure
```
grambo/
├── grap.py                 # New: Main parser CLI
├── gra.py                  # Refactored: Analysis tool
├── gras.py                 # Minimal changes
├── graw.py                 # No changes
├── lib/
│   ├── __init__.py
│   ├── entities/
│   │   ├── __init__.py
│   │   ├── base.py         # Entity base classes
│   │   ├── node.py         # NODE entity implementation
│   │   ├── state_transfer.py # STATE_TRANSFER entity
│   │   └── view.py         # VIEW entity
│   ├── patterns/
│   │   ├── __init__.py
│   │   ├── registry.py     # Pattern management
│   │   ├── matcher.py      # Pattern matching logic
│   │   └── learner.py      # Interactive learning
│   ├── relationships/
│   │   ├── __init__.py
│   │   ├── detector.py     # Relationship detection
│   │   └── inference.py    # Relationship inference
│   └── output/
│       ├── __init__.py
│       ├── serializer.py   # JSON/output formatting
│       └── compatibility.py # Backward compatibility
├── patterns/
│   ├── core/               # Built-in patterns
│   └── examples/           # Example pattern files
├── tests/
│   ├── test_entities.py
│   ├── test_patterns.py
│   ├── test_learning.py
│   └── fixtures/           # Test log files
└── docs/
    ├── entity_reference.md
    ├── pattern_authoring.md
    └── migration_guide.md
```

### 4.3 API Contracts

#### 4.3.1 Entity Interface
```python
class EntityProtocol(Protocol):
    def extract_from_line(self, line: str, context: ParsingContext) -> Optional['Entity']:
        """Extract entity from log line"""
        ...
    
    def validate_properties(self) -> bool:
        """Validate entity properties"""
        ...
    
    def infer_relationships(self, other_entities: List['Entity']) -> List[Relationship]:
        """Infer relationships with other entities"""
        ...
```

#### 4.3.2 Pattern Interface
```python
class PatternProtocol(Protocol):
    def match(self, line: str) -> Optional[PatternMatch]:
        """Match pattern against log line"""
        ...
    
    def extract_properties(self, match: PatternMatch) -> Dict[str, Any]:
        """Extract entity properties from match"""
        ...
    
    def calculate_confidence(self, match: PatternMatch) -> float:
        """Calculate match confidence"""
        ...
```

### 4.4 Configuration Management
```yaml
# ~/.grambo/config.yml
parser:
  interaction_mode: "CONFIRM"        # SILENT, CONFIRM, TEACH, BATCH
  confidence_threshold: 0.7
  auto_save_patterns: true
  pattern_directories:
    - "~/.grambo/patterns/core"
    - "~/.grambo/patterns/learned"
  
output:
  format: "json"                     # json, msgpack
  include_relationships: true
  include_metadata: true
  compatibility_mode: false          # Output format compatible with gra v1

performance:
  max_memory_mb: 1024
  batch_size: 1000
  enable_caching: true
```

---

## 5. QUALITY GATES

### 5.1 Unit Test Requirements
- **Coverage**: >90% for core entity and pattern logic
- **Performance**: Parse 10K lines in <5 seconds
- **Memory**: <100MB for 1M line log files

### 5.2 Integration Test Requirements
- **Compatibility**: Output works with existing `gras` without changes
- **Regression**: All existing test cases pass with new implementation
- **Accuracy**: >95% entity extraction accuracy on known log samples

### 5.3 User Experience Requirements
- **Learning Time**: New patterns teachable in <2 minutes
- **Error Recovery**: Graceful handling of unknown patterns
- **Progress Feedback**: Clear progress indication for large files

---

## 6. MIGRATION STRATEGY

### 6.1 Backward Compatibility
1. **Phase 1-3**: `grap` outputs JSON compatible with current `gras` input
2. **Phase 4**: `gra` refactored to use structured input
3. **Phase 5**: Optional migration to new output format

### 6.2 User Migration Path
1. **Week 1-2**: Beta testers use `grap` in parallel with `gra`
2. **Week 3-4**: Compare outputs, teach new patterns
3. **Week 5-6**: Switch primary workflow to `grap → gras → graw`
4. **Week 7-8**: Deprecate old `gra` parsing logic

### 6.3 Rollback Strategy
- Keep `gra` original parsing logic intact until Phase 4
- Version all pattern files for easy rollback
- Maintain compatibility shims for emergency fallback

---

## 7. DECISION LOG

| Date | Decision | Rationale | Impact |
|------|----------|-----------|---------|
| 2025-09-22 | Use Pydantic for entity validation | Type safety, automatic serialization | +Development speed, +Reliability |
| 2025-09-22 | YAML for pattern definitions | Human-readable, version control friendly | +Maintainability |
| 2025-09-22 | CLI-first interactive learning | Support engineer workflow compatibility | +User adoption |

---

## 8. RISK MITIGATION

### 8.1 High-Risk Areas
1. **Pattern Learning Complexity**: Start simple, iterate based on user feedback
2. **Performance Regression**: Profile early and often, optimize hot paths
3. **User Adoption**: Maintain strict backward compatibility until Phase 4

### 8.2 Contingency Plans
- **Learning System Too Complex**: Fall back to manual pattern authoring
- **Performance Issues**: Implement streaming parser, reduce memory usage
- **User Resistance**: Extend compatibility period, improve UX

---

## 9. SUCCESS METRICS

### 9.1 Technical Metrics
- **Parsing Accuracy**: >95% entity extraction rate
- **Performance**: <5 second parsing for typical log files
- **Pattern Coverage**: Handle 90% of real-world log variations

### 9.2 User Metrics
- **Adoption Rate**: 80% of users switch to new workflow within 4 weeks
- **Learning Efficiency**: Average 3 new patterns taught per user
- **Support Reduction**: 50% fewer "unknown log format" issues

---

**Document Status**: APPROVED FOR IMPLEMENTATION
**Next Review**: After Phase 1 completion
**Owner**: Grambo Development Team