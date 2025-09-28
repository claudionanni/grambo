#!/usr/bin/env python3
"""
QuorumEntity: Represents a Galera Quorum result block (timestamped, not immutable)
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from .base import Entity, EntityType

@dataclass(frozen=True)
class QuorumEntity(Entity):
    raw_lines: str = ""
    group_uuid: Optional[str] = None
    version: Optional[str] = None
    component: Optional[str] = None
    conf_id: Optional[str] = None
    quorum_members: Optional[str] = None
    act_id: Optional[str] = None
    last_appl: Optional[str] = None
    protocols: Optional[str] = None
    vote_policy: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, 'entity_type', EntityType.QUORUM)
        if not self.entity_id:
            base = f"quorum_{self.timestamp}_{self.group_uuid or ''}_{self.line_number}"
            import hashlib
            object.__setattr__(self, 'entity_id', hashlib.md5(base.encode()).hexdigest()[:12])
