# Node Name Detection - Quick Reference

## 🚨 Problem Symptoms
```
⚠️  WARNING: Duplicate node name 'xyz' detected!
🧭 Nodes: 1  (instead of expected 2+)
Missing SST arrows in visualization
Overlapping nodes in cluster diagram
```

## 🔍 Quick Diagnosis
```bash
# Check for conflicts
python3 check-node-mapping.py *.json

# Look for duplicate local_node_name
grep -h local_node_name *.json
```

## ⚡ Quick Fix
```bash
# Use explicit mapping
python3 grambo-cluster.py \
  --node real-node-1:file1.json \
  --node real-node-2:file2.json \
  --format=json > cluster.json
```

## 🧠 Why It Happens
- **Log ambiguity**: Multiple logs claim same node identity
- **Collection issues**: Files copied/centralized lose context  
- **Config problems**: Missing `wsrep_node_name` settings
- **Time overlap**: Same events from different node perspectives

## 🎯 Prevention
```sql
-- Set explicit node names in Galera
SET GLOBAL wsrep_node_name = 'db-node-01';
SHOW VARIABLES LIKE 'wsrep_node_%';
```

```bash
# Use clear file naming
python3 gramboo.py --format=json /logs/db01.log > db01.json
python3 gramboo.py --format=json /logs/db02.log > db02.json
```

## 📖 Full Documentation
- [TROUBLESHOOTING_NODE_DETECTION.md](TROUBLESHOOTING_NODE_DETECTION.md) - Complete guide
- [NODE_MAPPING_GUIDE.md](NODE_MAPPING_GUIDE.md) - Usage patterns
- [README.md](README.md#troubleshooting) - Main documentation
