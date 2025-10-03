# SST/IST Analysis - GRAA Improvements Needed

## Current GRAA SST Tracking Analysis

Based on analysis of the cl407 logs, GRAA is **missing several critical SST/IST events** that provide a complete picture of state transfer operations.

### What GRAA Currently Tracks ✅
1. **SST Request** - `requested state transfer` messages
2. **SST Completion** - `State transfer to X complete` messages  
3. **SST Failures** - `Process completed with error` messages
4. **Basic IST** - Some IST processing events

### What GRAA is Missing ❌

#### 1. **SST Script Start Events**
- **Donor side**: `WSREP_SST: [INFO] mariabackup SST started on donor`
- **Joiner side**: `WSREP_SST: [INFO] mariabackup SST started on joiner`
- **Impact**: Can't tell when actual SST operations begin vs just requests

#### 2. **SST Script Completion Events** 
- **Donor side**: `WSREP_SST: [INFO] mariabackup SST completed on donor`
- **Joiner side**: `WSREP_SST: [INFO] mariabackup SST completed on joiner`
- **Impact**: Missing actual script-level success/failure status

#### 3. **SST Received Events**
- **Pattern**: `WSREP: SST received:` (with GTID position)
- **Impact**: Can't track final position received by joiner

#### 4. **SST Success Events**
- **Pattern**: `WSREP: SST succeeded for position`
- **Impact**: Missing final success confirmation

#### 5. **Detailed IST Events**
- **IST Preparation**: `Prepared IST receiver for X-Y`
- **IST Progress**: `Receiving IST... X% (Y/Z events) complete`
- **IST Received**: `IST received: UUID:seqno`
- **Impact**: Missing granular IST tracking

## Real-World Example from Latest Session (2025-10-03 10:58)

### What Actually Happened (Complete Flow):
```
10:58:25 - [REQUEST] Member 0.0 (NODE_21407) requested state transfer
10:58:25 - [DONOR_START] WSREP_SST: mariabackup SST started on donor  
10:58:24 - [JOINER_START] WSREP_SST: mariabackup SST started on joiner
10:58:44 - [DONOR_COMPLETE] WSREP_SST: mariabackup SST completed on donor
10:58:44 - [JOINER_COMPLETE] WSREP_SST: mariabackup SST completed on joiner
10:58:44 - [SST_SENT] WSREP: SST sent: 9b7675e6-a036-11f0-86ed-5bd5eda57819:27
10:58:44 - [COMPLETION] WSREP: State transfer to 0.0 (NODE_21407) complete
10:58:44 - [RECEIVED] WSREP: SST received
10:58:48 - [FINAL_POSITION] WSREP: SST received: 9b7675e6-a036-11f0-86ed-5bd5eda57819:31
10:58:48 - [SUCCESS] WSREP: SST succeeded for position 9b7675e6-a036-11f0-86ed-5bd5eda57819:31
10:58:48 - [IST_START] WSREP: Receiving IST... 0.0% (0/7 events) complete
10:58:48 - [IST_COMPLETE] WSREP: Receiving IST... 100.0% (7/7 events) complete
```

### What GRAA Currently Tracks:
```
10:58:25 - [REQUEST] Member 0.0 (NODE_21407) requested state transfer  
10:58:44 - [COMPLETION] WSREP: State transfer to 0.0 (NODE_21407) complete
```

**Missing**: 10+ critical events showing actual SST script execution, positions, and IST details!

## Scenarios GRAA Should Handle

### 1. **Request but No Start** (SST Request Never Executed)
```
- Request: "Member X requested state transfer"
- Status: REQUESTED_ONLY (no start event found)
```

### 2. **Start but No Completion** (SST Started but Hung/Incomplete)
```
- Request: "Member X requested state transfer" 
- Start: "mariabackup SST started on donor/joiner"
- Status: STARTED_NO_COMPLETION (missing completion info)
```

### 3. **Script Success vs Transfer Success** (Script completed but transfer failed)
```
- Script: "mariabackup SST completed on donor"
- Transfer: "State transfer failed: Invalid argument"
- Status: SCRIPT_SUCCESS_TRANSFER_FAILED
```

### 4. **IST After SST** (Combined SST+IST operations)
```
- SST: Complete state snapshot transfer
- IST: Additional incremental updates
- Status: SST_WITH_IST_FOLLOWUP
```

## Proposed GRAA Improvements

### 1. **Enhanced Event Detection Patterns**
```python
SST_PATTERNS = {
    'request': r'Member.*requested state transfer.*Selected.*as donor',
    'donor_start': r'WSREP_SST:.*SST started on donor',
    'joiner_start': r'WSREP_SST:.*SST started on joiner', 
    'donor_complete': r'WSREP_SST:.*SST completed on donor',
    'joiner_complete': r'WSREP_SST:.*SST completed on joiner',
    'sst_sent': r'WSREP: SST sent:',
    'sst_received': r'WSREP: SST received:',
    'sst_succeeded': r'WSREP: SST succeeded for position',
    'transfer_complete': r'State transfer.*complete',
    'transfer_failed': r'State transfer.*failed'
}

IST_PATTERNS = {
    'ist_prepared': r'Prepared IST receiver for',
    'ist_receiving': r'Receiving IST.*complete',
    'ist_received': r'IST received:'
}
```

### 2. **Enhanced Session Status Types**
- `REQUESTED_ONLY` - Request made but no start detected
- `DONOR_STARTED` - Donor script started  
- `JOINER_STARTED` - Joiner script started
- `SCRIPTS_COMPLETED` - Both scripts completed
- `TRANSFER_SUCCESS` - Full transfer successful
- `SCRIPT_SUCCESS_TRANSFER_FAILED` - Scripts ok but transfer failed
- `INCOMPLETE` - Missing completion information

### 3. **Multi-Node Correlation**
Correlate events across donor and joiner logs for complete picture.

### 4. **Intuitive Output Format**
```
[19] SST SESSION — COMPLETED
    Request: 2025-10-03 10:58:25 | NODE_21407 → NODE_11407  
    Donor Script: STARTED 10:58:25 → COMPLETED 10:58:44 (19s)
    Joiner Script: STARTED 10:58:24 → COMPLETED 10:58:44 (20s) 
    Transfer: SUCCESS | Position: 9b7675e6-a036-11f0-86ed-5bd5eda57819:31
    Follow-up IST: 7 events (0.0% → 100.0%)
    Total Duration: 23s (script execution) + 4s (IST)
```

This would provide the complete, intuitive overview you requested!