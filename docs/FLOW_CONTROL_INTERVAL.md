# Galera Flow Control Interval: Complete Analysis

## Source Code Reference
Based on `galera-4-26.4.23/gcs/src/gcs.cpp` (lines 919-931, 936-955)

---

## The Message

```
WSREP: Flow-control interval: [16, 16]
```

**Format:** `[lower_limit, upper_limit]`

---

## What It Means

This message displays the **flow control thresholds** for this node's slave queue (receive queue).

### The Two Limits

1. **Lower Limit (first number)**: 
   - When slave queue length **drops below** this value
   - Node sends **FC_CONT** (continue) signal to other nodes
   - "I've caught up, you can send more"

2. **Upper Limit (second number)**:
   - When slave queue length **exceeds** this value  
   - Node sends **FC_STOP** signal to other nodes
   - "I'm falling behind, please slow down"

---

## Flow Control Mechanism

```
Slave Queue Depth (writesets waiting to be applied):

                       ┌─── FC_STOP sent ───┐
                       │                     │
  Queue = 20 ───────────────────────────────┤ Node overwhelmed
  Queue = 16 ═══════════════════════════════╪═ upper_limit
                       │ Normal zone        │
  Queue = 16 ═══════════════════════════════╪═ lower_limit  
  Queue = 10 ───────────────────────────────┤ Node catching up
                       │                     │
                       └─── FC_CONT sent ───┘
  Queue = 0
```

### Decision Logic (from source code)

**Send FC_STOP when:**
```c
stop_count <= 0 && 
stop_sent <= 0 && 
queue_len > (upper_limit + fc_offset) &&
state <= max_fc_state
```

**Send FC_CONT when:**
```c
stop_sent > 0 && 
queue_len < (lower_limit + fc_offset)
```

---

## How Limits Are Calculated

From `_set_fc_limits()` function:

```c
// Scaling factor based on cluster size
double fn = fc_single_primary ? 1.0 : sqrt(memb_num);

// Calculate limits
upper_limit = fc_base_limit * fn + 0.5;
lower_limit = upper_limit * fc_resume_factor + 0.5;
```

### Parameters

| Parameter | Config Name | Default | Range | Description |
|-----------|-------------|---------|-------|-------------|
| fc_base_limit | `gcs.fc_limit` | 16 | 0-∞ | Base limit for slave queue depth |
| fc_resume_factor | `gcs.fc_factor` | 1.0 | 0.0-1.0 | Fraction of upper limit for resume |
| fc_single_primary | `gcs.fc_single_primary` | no | yes/no | Use flat profile (no scaling) |
| memb_num | (auto) | cluster size | - | Number of cluster members |

---

## Example Calculations

### [16, 16] - Your Case

**Scenario:** Single node OR fc_single_primary=yes
```
fn = 1.0
upper_limit = 16 * 1.0 = 16
lower_limit = 16 * 1.0 = 16
```

**Meaning:**
- Node at **queue=16**: Minimal hysteresis, aggressive flow control
- Tight control prevents queue growth
- Common in single-node or master-slave configurations

### Multi-Node Clusters (fc_single_primary=no)

**2-node cluster:**
```
fn = sqrt(2) ≈ 1.414
upper_limit = 16 * 1.414 ≈ 23
lower_limit = 23 * 1.0 = 23
Interval: [23, 23]
```

**3-node cluster:**
```
fn = sqrt(3) ≈ 1.732
upper_limit = 16 * 1.732 ≈ 28
lower_limit = 28 * 1.0 = 28
Interval: [28, 28]
```

**4-node cluster:**
```
fn = sqrt(4) = 2.0
upper_limit = 16 * 2.0 = 32
lower_limit = 32 * 1.0 = 32
Interval: [32, 32]
```

### With Different fc_factor

**3-node cluster with fc_factor=0.8:**
```
fn = sqrt(3) ≈ 1.732
upper_limit = 16 * 1.732 ≈ 28
lower_limit = 28 * 0.8 = 22.4 ≈ 22
Interval: [22, 28]
```

**Effect:** More hysteresis, less aggressive flow control

---

## When This Message Appears

The message is logged when `_set_fc_limits()` is called, which happens:

1. **Node becomes SYNCED** (lines 1052-1056)
   - After completing SST/IST
   - Node is ready to apply writesets
   - Flow control is enabled

2. **View changes** (lines 2499, 2532)
   - When cluster membership changes
   - Nodes join or leave
   - Limits recalculated based on new member count

3. **Flow control is reset**
   - After configuration changes
   - During state transitions

---

## The Slave Queue

**What it contains:**
- Writesets received from other cluster nodes
- Transactions waiting to be applied locally
- Ordered by global sequence number (seqno)

**Why it grows:**
- Local node applying slower than cluster produces
- Heavy local workload (queries, disk I/O)
- Replication lag
- Hardware limitations (CPU, disk, memory)

**What happens when it's full:**
- Node sends FC_STOP to all cluster members
- Other nodes pause sending writesets
- Local node catches up
- Node sends FC_CONT when queue drops
- Normal operation resumes

---

## Flow Control Benefits

### Prevents:
- Memory exhaustion from unbounded queue growth
- Excessive replication lag
- Cluster fragmentation
- Out-of-memory errors
- Node crashes due to resource exhaustion

### Ensures:
- Cluster operates at speed of slowest node
- All nodes stay reasonably synchronized
- Predictable memory usage
- Graceful degradation under load

---

## Performance Implications

### [16, 16] Configuration

**Pros:**
- Tight control over replication lag
- Prevents queue from growing large
- Lower memory usage
- Quick detection of slow nodes

**Cons:**
- Aggressive flow control
- May pause fast nodes frequently
- Reduced cluster throughput
- Sensitive to temporary slowdowns

### Tuning Considerations

**Increase fc_limit** (e.g., 32, 64, 100):
- Allows larger queue before triggering FC
- Higher throughput for fast clusters
- More memory usage
- Larger potential lag

**Decrease fc_factor** (e.g., 0.5, 0.75):
- Creates hysteresis between STOP and CONT
- Reduces FC oscillation (stop/start cycles)
- Smoother operation
- More predictable behavior

---

## Related Configuration

### Galera Parameters

```ini
# Flow control base limit
wsrep_provider_options="gcs.fc_limit=16"

# Flow control resume factor  
wsrep_provider_options="gcs.fc_factor=1.0"

# Single primary mode (master-slave)
wsrep_provider_options="gcs.fc_single_primary=NO"

# Flow control debug logging
wsrep_provider_options="gcs.fc_debug=0"
```

### Monitoring

Check these status variables:
```sql
SHOW STATUS LIKE 'wsrep_flow_control%';
```

- `wsrep_flow_control_paused` - Fraction of time paused (0.0 to 1.0)
- `wsrep_flow_control_sent` - FC_STOP messages sent
- `wsrep_flow_control_recv` - FC messages received

---

## Troubleshooting

### High Flow Control Paused (> 0.1)

**Symptoms:** `wsrep_flow_control_paused = 0.3` (30% of time paused)

**Causes:**
- Slow disk I/O on one or more nodes
- Insufficient CPU resources
- Large transactions
- Lock contention
- Heavy SELECT queries competing with replication

**Solutions:**
1. Identify slow node(s)
2. Optimize queries and indexes
3. Increase hardware resources
4. Tune InnoDB settings (buffer pool, I/O capacity)
5. Consider increasing fc_limit (carefully)

### Frequent Flow Control Events

**Symptoms:** Many "Flow-control interval" messages, oscillating

**Possible causes:**
- fc_limit too low
- No hysteresis (upper = lower)
- Bursty workload

**Solutions:**
1. Increase fc_limit
2. Set fc_factor < 1.0 (e.g., 0.8) to create hysteresis
3. Smooth out workload if possible

---

## Summary

**WSREP: Flow-control interval: [16, 16]** tells you:

- ✅ Flow control is **active and configured**
- ✅ Upper threshold: **16 writesets** in slave queue
- ✅ Lower threshold: **16 writesets** (resume point)
- ✅ Configuration: **Default values** for single-node or master-slave
- ✅ Behavior: **Aggressive** flow control with minimal hysteresis

**This is normal and expected behavior** - it's Galera protecting itself from replication lag and resource exhaustion.

---

## Source Code References

- `gcs/src/gcs.cpp:919-931` - _set_fc_limits()
- `gcs/src/gcs.cpp:936-955` - gcs_handle_flow_control()
- `gcs/src/gcs_params.cpp` - Parameter defaults
- `gcs/src/gcs_params.hpp` - Parameter declarations
