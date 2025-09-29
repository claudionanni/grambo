# GRAP Validation Report - Mon Sep 29 08:10:35 PM CEST 2025

## Test Execution Summary

**Validation Suite Version**: 1.0  
**Execution Time**: Mon Sep 29 08:10:35 PM CEST 2025  
**Project Root**: /home/claudio/Projects/GITHUB/grambo  

## Test Results

### Node State Reliability Analysis
```
Node State Reliability Analysis
==================================================
=== EXTRACTING RAW LOG SHIFTING EVENTS ===
Found 106 raw shifting events

=== EXTRACTING GRAP NODE_STATE ENTITIES ===
Found 201 GRAP node_state entities

=== ANALYZING STATE TRANSITIONS ===

Raw events grouped into 71 timestamp/node combinations
GRAP states grouped into 117 timestamp/node combinations

Matching timestamp/node combinations: 56
Raw-only combinations: 15
GRAP-only combinations: 61

=== DETAILED COMPARISON ===

Analyzing first 10 matching events:

1. 2025-09-22 20:42:11 - NODE_11407
   Raw events: 2
     CLOSED -> OPEN
     JOINED -> SYNCED
   GRAP states: 2
     SYNCED (confidence: 1.0)
     SYNCED (confidence: 1.0)

2. 2025-09-22 20:46:30 - NODE_11407
   Raw events: 1
     SYNCED -> CLOSED
   GRAP states: 1
     CLOSED (confidence: 1.0)

3. 2025-09-22 20:46:40 - NODE_11407
   Raw events: 2
     CLOSED -> OPEN
     JOINED -> SYNCED
   GRAP states: 3
     OPEN (confidence: 1.0)
     SYNCED (confidence: 1.0)
     SYNCED (confidence: 1.0)

4. 2025-09-22 22:27:03 - NODE_11407
   Raw events: 2
     CLOSED -> OPEN
     JOINED -> SYNCED
   GRAP states: 3
     OPEN (confidence: 1.0)
     SYNCED (confidence: 1.0)
     SYNCED (confidence: 1.0)

5. 2025-09-22 22:27:19 - NODE_11407
   Raw events: 2
     CLOSED -> OPEN
     JOINED -> SYNCED
   GRAP states: 3
     OPEN (confidence: 1.0)
     SYNCED (confidence: 1.0)
     SYNCED (confidence: 1.0)

6. 2025-09-22 22:28:06 - NODE_11407
   Raw events: 2
     CLOSED -> OPEN
     JOINED -> SYNCED
   GRAP states: 3
     OPEN (confidence: 1.0)
     SYNCED (confidence: 1.0)
     SYNCED (confidence: 1.0)

7. 2025-09-22 22:30:41 - NODE_11407
   Raw events: 1
     SYNCED -> DONOR/DESYNCED
   GRAP states: 1
     DONOR (confidence: 1.0)

8. 2025-09-22 22:30:52 - NODE_11407
   Raw events: 1
     JOINED -> SYNCED
   GRAP states: 3
     SYNCED (confidence: 1.0)
     SYNCED (confidence: 1.0)
     SYNCED (confidence: 1.0)

9. 2025-09-22 22:31:58 - NODE_11407
   Raw events: 1
     SYNCED -> CLOSED
   GRAP states: 1
     CLOSED (confidence: 1.0)

10. 2025-09-22 22:32:15 - NODE_11407
   Raw events: 2
     CLOSED -> OPEN
     JOINED -> SYNCED
   GRAP states: 3
     OPEN (confidence: 1.0)
     SYNCED (confidence: 1.0)
     SYNCED (confidence: 1.0)

=== NODE STATE STATISTICS ===

State transition counts by node:
  NODE_11407: Raw=57, GRAP=92, Ratio=1.6140350877192982
  NODE_21407: Raw=5, GRAP=25, Ratio=5.0
  NODE_31407: Raw=44, GRAP=84, Ratio=1.9090909090909092

=== ANALYSIS COMPLETE ===
```

### Pattern Analysis Details  
```
Detailed Node State Pattern Analysis
============================================================
=== ANALYZING GRAP PATTERN USAGE ===

Node state patterns found:
  : 201 instances

=== PATTERN VS SHIFTING ANALYSIS ===
=== ANALYZING GRAP PATTERN USAGE ===

Node state patterns found:
  : 201 instances

--- Pattern:  (201 entities) ---
  ✓ SHIFTING: 2025-09-22 20:42:11 NODE_11407 -> SYNCED
    Raw: 2025-09-22 20:42:11 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
  ✓ SHIFTING: 2025-09-22 20:42:11 NODE_11407 -> SYNCED
    Raw: 2025-09-22 20:42:11 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 1)...
  ? NON-SHIFTING: 2025-09-22 20:42:14 NODE_11407 -> SYNCED
    Raw: 2025-09-22 20:42:14 2 [Note] WSREP: Synchronized with group, ready for connections...
  ✓ SHIFTING: 2025-09-22 20:46:30 NODE_11407 -> CLOSED
    Raw: 2025-09-22 20:46:30 0 [Note] WSREP: Shifting SYNCED -> CLOSED (TO: 2)...
  ✓ SHIFTING: 2025-09-22 20:46:40 NODE_11407 -> OPEN
    Raw: 2025-09-22 20:46:40 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)...
  Summary: 90 shifting, 111 non-shifting

=== STATE EXTRACTION METHODS ===
Extraction method distribution:
  membership_view: 69 entities
    Example: 2025-09-22 20:42:11 NODE_11407 -> SYNCED
      Raw: 2025-09-22 20:42:11 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
    Example: 2025-09-22 20:46:40 NODE_11407 -> SYNCED
      Raw: 2025-09-22 20:46:40 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
    Example: 2025-09-22 22:27:03 NODE_11407 -> SYNCED
      Raw: 2025-09-22 22:27:03 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
  shifting_transition: 90 entities
    Example: 2025-09-22 20:42:11 NODE_11407 -> SYNCED
      Raw: 2025-09-22 20:42:11 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 1)...
    Example: 2025-09-22 20:46:30 NODE_11407 -> CLOSED
      Raw: 2025-09-22 20:46:30 0 [Note] WSREP: Shifting SYNCED -> CLOSED (TO: 2)...
    Example: 2025-09-22 20:46:40 NODE_11407 -> OPEN
      Raw: 2025-09-22 20:46:40 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)...
  other_pattern: 42 entities
    Example: 2025-09-22 20:42:14 NODE_11407 -> SYNCED
      Raw: 2025-09-22 20:42:14 2 [Note] WSREP: Synchronized with group, ready for connectio...
    Example: 2025-09-22 20:46:49 NODE_11407 -> SYNCED
      Raw: 2025-09-22 20:46:49 2 [Note] WSREP: Synchronized with group, ready for connectio...
    Example: 2025-09-22 22:28:10 NODE_11407 -> SYNCED
      Raw: 2025-09-22 22:28:10 2 [Note] WSREP: Synchronized with group, ready for connectio...

=== DUPLICATE EXTRACTION ANALYSIS ===
Found 40 timestamp/node combinations with multiple state entities

1. 2025-09-22 20:42:11 - NODE_11407 (2 entities)
   1) SYNCED (pattern: )
      Line: 2025-09-22 20:42:11 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
   2) SYNCED (pattern: )
      Line: 2025-09-22 20:42:11 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 1)...

2. 2025-09-22 20:46:40 - NODE_11407 (3 entities)
   1) OPEN (pattern: )
      Line: 2025-09-22 20:46:40 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)...
   2) SYNCED (pattern: )
      Line: 2025-09-22 20:46:40 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
   3) SYNCED (pattern: )
      Line: 2025-09-22 20:46:40 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 3)...

3. 2025-09-22 22:27:03 - NODE_11407 (3 entities)
   1) OPEN (pattern: )
      Line: 2025-09-22 22:27:03 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)...
   2) SYNCED (pattern: )
      Line: 2025-09-22 22:27:03 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
   3) SYNCED (pattern: )
      Line: 2025-09-22 22:27:03 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 4)...

4. 2025-09-22 22:27:19 - NODE_11407 (3 entities)
   1) OPEN (pattern: )
      Line: 2025-09-22 22:27:19 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)...
   2) SYNCED (pattern: )
      Line: 2025-09-22 22:27:19 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
   3) SYNCED (pattern: )
      Line: 2025-09-22 22:27:19 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 1)...

5. 2025-09-22 22:28:06 - NODE_11407 (3 entities)
   1) OPEN (pattern: )
      Line: 2025-09-22 22:28:06 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)...
   2) SYNCED (pattern: )
      Line: 2025-09-22 22:28:06 0 [Note] WSREP: Member 0.0 (NODE_11407) synced with group....
   3) SYNCED (pattern: )
      Line: 2025-09-22 22:28:06 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 1)...

=== ANALYSIS COMPLETE ===
```

### UUID Consistency Validation
```
UUID Consistency Validation
==================================================
=== UUID CONSISTENCY VALIDATION ===
Found 195 node_state entities with UUIDs
Found 386 view entities

=== UUID CONSISTENCY ANALYSIS ===

Node: NODE_11407
  Total UUID assignments: 88
  Unique UUIDs: 7
  ⚠️  WARNING: Node has multiple UUIDs over time
    3a42f33d-97f3-11f0-ae74-8ea804e8387d: 63 occurrences, first seen: 2025-09-22 22:32:26
    89c65b64-97f2-11f0-87c2-22481ca21bac: 3 occurrences, first seen: 2025-09-22 22:28:06
    2c7ff87c-9d43-11f0-ae9b-c645351e7c4e: 1 occurrences, first seen: 2025-09-29 16:47:18
    89c65b64-97f2-11f0-87c3-22481ca21bac: 9 occurrences, first seen: 2025-09-22 22:28:10
    7a30da88-97e4-11f0-aef9-7e66bbcd8637: 3 occurrences, first seen: 2025-09-22 22:27:19
    d9c6d6f5-97e3-11f0-abb6-a63756ae0ed5: 5 occurrences, first seen: 2025-09-22 20:42:14
    7a30da88-97e4-11f0-aef8-7e66bbcd8637: 4 occurrences, first seen: 2025-09-22 20:46:49
  Time span: 2025-09-22 20:42:14 to 2025-09-29 16:47:18

Node: NODE_21407
  Total UUID assignments: 25
  Unique UUIDs: 5
  ⚠️  WARNING: Node has multiple UUIDs over time
    3a42f33d-97f3-11f0-ae74-8ea804e8387d: 4 occurrences, first seen: 2025-09-25 13:54:33
    3e3cbf8a-9d43-11f0-a47a-c712da0bb254: 8 occurrences, first seen: 2025-09-29 16:47:39
    4f28049d-97f4-11f0-8901-3e7557106ab8: 11 occurrences, first seen: 2025-09-22 22:40:01
    64eaad05-97f3-11f0-8a2a-069ebc969303: 1 occurrences, first seen: 2025-09-22 22:33:28
    01072cd7-97f3-11f0-b1d4-b2e5c7499804: 1 occurrences, first seen: 2025-09-22 22:30:41
  Time span: 2025-09-22 22:30:41 to 2025-09-29 16:47:59

Node: NODE_31407
  Total UUID assignments: 82
  Unique UUIDs: 11
  ⚠️  WARNING: Node has multiple UUIDs over time
    659a6fbe-9a06-11f0-9e8a-92d4b1e1ffc9: 2 occurrences, first seen: 2025-09-25 13:54:32
    c2ff8d59-9a22-11f0-85fd-c207fa08e39e: 1 occurrences, first seen: 2025-09-25 17:17:36
    7b611235-9a29-11f0-9bc9-8efb3da5f47d: 10 occurrences, first seen: 2025-09-25 18:05:41
    2c7ff87c-9d43-11f0-ae9b-c645351e7c4e: 12 occurrences, first seen: 2025-09-29 16:47:09
    b9008526-9a29-11f0-a61b-fefdf3d11811: 27 occurrences, first seen: 2025-09-25 18:07:25
    eacab148-9895-11f0-81f0-1a331c96159d: 3 occurrences, first seen: 2025-09-23 17:56:51
    6d6256fd-9a23-11f0-b15e-83e09e5959f9: 10 occurrences, first seen: 2025-09-25 17:22:20
    383ebaa7-9a29-11f0-b484-c67777546e1a: 10 occurrences, first seen: 2025-09-25 18:03:49
    37914f28-9858-11f0-8155-3f12cfb0219d: 2 occurrences, first seen: 2025-09-23 10:35:11
    01df0404-9858-11f0-a8e2-63455f1a5e9b: 2 occurrences, first seen: 2025-09-23 10:33:41
    87155f75-97f4-11f0-b390-d257498a9318: 3 occurrences, first seen: 2025-09-22 22:41:35
  Time span: 2025-09-22 22:41:35 to 2025-09-29 16:47:59

=== VIEW MEMBER CONSISTENCY ===
Found 386 view entities
Nodes in views: []

=== UUID TIMELINE GENERATION ===
Generated timeline with 321 UUID-related events

Sample timeline entries:
  1. 2025-09-22 20:42:11 - d9c6d6f5-abb6 (view) -> d9c6d6f5...
  2. 2025-09-22 20:42:14 - NODE_11407 (node_state) -> d9c6d6f5...
  3. 2025-09-22 20:46:30 - NODE_11407 (node_state) -> d9c6d6f5...
  4. 2025-09-22 20:46:40 - NODE_11407 (view) -> 7a30da88...
  5. 2025-09-22 20:46:40 - NODE_11407 (node_state) -> d9c6d6f5...
  6. 2025-09-22 20:46:40 - NODE_11407 (node_state) -> d9c6d6f5...
  7. 2025-09-22 20:46:40 - NODE_11407 (node_state) -> d9c6d6f5...
  8. 2025-09-22 20:46:49 - NODE_11407 (node_state) -> 7a30da88...
  9. 2025-09-22 22:27:03 - NODE_11407 (view) -> 7a30da88...
  10. 2025-09-22 22:27:03 - NODE_11407 (node_state) -> 7a30da88...

=== VALIDATION SUMMARY ===
UUID consistency issues: 3
View consistency issues: 0
Timeline events generated: 321
⚠️ UUID consistency validation found issues
```

### Entity Coverage Analysis
```
Entity Coverage Validation
==================================================
=== ENTITY COVERAGE ANALYSIS ===
Total entities extracted: 1179
Entity types found: 8

Entity type distribution:
  error               :  474 ( 40.2%)
  view                :  386 ( 32.7%)
  node_state          :  201 ( 17.0%)
  quorum              :   63 (  5.3%)
  sst                 :   45 (  3.8%)
  ist                 :    4 (  0.3%)
  cluster             :    3 (  0.3%)
  node                :    3 (  0.3%)

=== EXPECTED ENTITY VALIDATION ===
  cluster        :    3 (min:  1) ✅ PASS - Cluster identification
  node_state     :  201 (min: 10) ✅ PASS - Node state transitions
  view           :  386 (min:  5) ✅ PASS - Cluster membership views
  sst_event      :    0 (min:  0) ✅ PASS - State Snapshot Transfer events
  ist_event      :    0 (min:  0) ✅ PASS - Incremental State Transfer events
  error          :  474 (min:  0) ✅ PASS - Error conditions
  warning        :    0 (min:  0) ✅ PASS - Warning messages

Unexpected entity types found:
  quorum         :   63 - May indicate new patterns or extraction issues
  sst            :   45 - May indicate new patterns or extraction issues
  ist            :    4 - May indicate new patterns or extraction issues
  node           :    3 - May indicate new patterns or extraction issues

=== ENTITY QUALITY ANALYSIS ===

cluster (3 entities):
  Average confidence: 1.00
  Timestamp coverage: 100.0%
  Raw line coverage: 0.0%
  Line number coverage: 0.0%
  ⚠️ Quality issues: Missing raw lines

quorum (63 entities):
  Average confidence: 1.00
  Timestamp coverage: 85.7%
  Raw line coverage: 0.0%
  Line number coverage: 100.0%
  ⚠️ Quality issues: Missing timestamps, Missing raw lines

error (474 entities):
  Average confidence: 1.00
  Timestamp coverage: 100.0%
  Raw line coverage: 100.0%
  Line number coverage: 100.0%
  ✅ Good quality metrics

view (386 entities):
  Average confidence: 1.00
  Timestamp coverage: 100.0%
  Raw line coverage: 100.0%
  Line number coverage: 100.0%
  ✅ Good quality metrics

node_state (201 entities):
  Average confidence: 1.00
  Timestamp coverage: 100.0%
  Raw line coverage: 100.0%
  Line number coverage: 100.0%
  ✅ Good quality metrics

sst (45 entities):
  Average confidence: 1.00
  Timestamp coverage: 62.2%
  Raw line coverage: 100.0%
  Line number coverage: 100.0%
  ⚠️ Quality issues: Missing timestamps

node (3 entities):
  Average confidence: 1.00
  Timestamp coverage: 0.0%
  Raw line coverage: 0.0%
  Line number coverage: 0.0%
  ⚠️ Quality issues: Missing timestamps, Missing raw lines

ist (4 entities):
  Average confidence: 1.00
  Timestamp coverage: 100.0%
  Raw line coverage: 100.0%
  Line number coverage: 100.0%
  ✅ Good quality metrics

=== TEMPORAL COVERAGE ANALYSIS ===
Temporal span: 2025-09-22 20:41:31 to 2025-09-29 16:47:59
Total timestamped events: 1150
Events distributed across 8 days:
  2025-09-22: 163 events
  2025-09-23: 99 events
  2025-09-24: 53 events
  2025-09-25: 257 events
  2025-09-26: 101 events
  2025-09-27: 227 events
  2025-09-28: 107 events
  2025-09-29: 143 events

=== OVERALL ASSESSMENT ===
Entity type validations: 7/7 passed
✅ Entity coverage validation PASSED
```

## Quick Assessment

### Key Metrics
- **Raw shifting events detected**: 106
- **GRAP node_state entities extracted**: 201
- **Entity extraction ratio**: 1.89x
- **Matching timestamp/node combinations**: 56

### Assessment
✅ **PASSED**: Extraction ratio within expected range (1.89x)
✅ **PASSED**: Good timestamp/node combination matching (56)

## Files Generated
- `validation_report_20250929_201035.md.reliability` - Raw reliability test output
- `validation_report_20250929_201035.md.patterns` - Raw pattern analysis output  
- `validation_report_20250929_201035.md` - This summary report

## Next Steps
1. Review detailed analysis in `tests/validation/NODE_STATE_RELIABILITY_REPORT.md`
2. Address any failed tests or warnings
3. Archive this report for regression testing comparison

*Generated by GRAP Quality Validation Suite v1.0*
