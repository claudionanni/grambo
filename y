GRAP V2:
    {
      "entity_id": "node_state_NODE_11407_SYNCED_721",
      "entity_type": "node_state",
      "timestamp": "2025-09-22 22:28:06",
      "line_number": 721,
      "raw_line": "2025-09-22 22:28:06 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 1)",
      "log_source": "cl407/error.11407.log",
      "confidence": 1.0,
      "pattern_name": "",
      "extraction_method": "local_pattern",
      "cluster_ref": "cluster_d9c70dcb",
      "validated": false,
      "validation_notes": "",
      "node_name": "NODE_11407",
      "node_state": "SYNCED",
      "timestamp_index": 6
    },

GRAP V3:
    {
      "entity_type": "node_state",
      "entity_id": "node_state_1079",
      "raw_line": "2025-09-22 22:32:15 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 4)",
      "line_number": 1079,
      "log_source": "cl407/error.11407.log",
      "confidence": 0.98,
      "pattern_name": "node_state_shifting",
      "extraction_method": "pattern_match",
      "timestamp": "2025-09-22 22:32:15",
      "from_state": "JOINED",
      "to_state": "SYNCED",
      "transition_type": "LOCAL_SHIFT",
      "node_name": "NODE_11407",
      "node_uuid": "89c65b64-97f2-11f0-87c3-22481ca21bac",
      "timestamp_index": 3
    }
