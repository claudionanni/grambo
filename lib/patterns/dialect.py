"""
Dialect detection and management for Galera log parsing

This module provides dialect-aware parsing by detecting MariaDB/MySQL versions
and log formats from log content, with fallback to sensible defaults.
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import logging


class DialectType(Enum):
    """Supported dialect types"""
    MARIADB_10_6 = "mariadb-10.6"
    MARIADB_11_4 = "mariadb-11.4"
    MARIADB_11_0 = "mariadb-11.0"
    MYSQL_8_0 = "mysql-8.0"
    PXC_8_0 = "pxc-8.0"
    DEFAULT = "default"


@dataclass
class DialectInfo:
    """Information about a detected dialect"""
    dialect_type: DialectType
    version: str
    confidence: float
    detection_method: str
    features: List[str]


class DialectDetector:
    """
    Detects dialect and version from log content
    
    Uses multiple detection methods:
    1. Version headers in logs
    2. Log message format patterns  
    3. WSREP/Galera version strings
    4. Feature-specific patterns
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._detection_patterns = self._build_detection_patterns()
        
    def _build_detection_patterns(self) -> Dict[str, List[Tuple[re.Pattern, DialectType, float]]]:
        """Build compiled regex patterns for dialect detection"""
        patterns = {
            'version_headers': [
                # MariaDB version patterns
                (re.compile(r'MariaDB\s+(\d+)\.(\d+)\.(\d+)', re.IGNORECASE), 
                 self._determine_mariadb_dialect, 0.9),
                (re.compile(r'mariadb.*version.*(\d+)\.(\d+)', re.IGNORECASE),
                 self._determine_mariadb_dialect, 0.8),
                
                # MySQL/PXC patterns
                (re.compile(r'MySQL\s+(\d+)\.(\d+)\.(\d+)', re.IGNORECASE),
                 DialectType.MYSQL_8_0, 0.8),
                (re.compile(r'Percona\s+XtraDB\s+Cluster.*(\d+)\.(\d+)', re.IGNORECASE),
                 DialectType.PXC_8_0, 0.9),
            ],
            
            'galera_version': [
                # Galera version patterns that indicate MariaDB vs others
                (re.compile(r'WSREP:\s+Galera\s+(\d+)\.(\d+)\.(\d+)', re.IGNORECASE),
                 self._determine_galera_dialect, 0.7),
                (re.compile(r'wsrep_provider_version.*(\d+)\.(\d+)', re.IGNORECASE),
                 self._determine_galera_dialect, 0.6),
            ],
            
            'log_format': [
                # MariaDB 10.6+ specific formats
                (re.compile(r'WSREP:\s+view\(view_id\((?:PRIM|NON_PRIM),[^,]+,\d+\)', re.IGNORECASE),
                 DialectType.MARIADB_10_6, 0.8),
                
                # MariaDB 11.4+ specific formats  
                (re.compile(r'WSREP:\s+.*allowlist\s+service', re.IGNORECASE),
                 DialectType.MARIADB_11_4, 0.8),
                
                # Legacy format indicators
                (re.compile(r'WSREP:\s+New\s+PRIMARY\s+view:', re.IGNORECASE),
                 DialectType.DEFAULT, 0.5),
            ],
            
            'features': [
                # Feature-specific patterns that help identify versions
                (re.compile(r'TLS\s+service\s+for\s+WSREP', re.IGNORECASE),
                 DialectType.MARIADB_11_4, 0.6),
                (re.compile(r'CRC-32C:\s+using.*acceleration', re.IGNORECASE),
                 DialectType.MARIADB_10_6, 0.5),
            ]
        }
        return patterns
    
    def _determine_mariadb_dialect(self, match: re.Match) -> DialectType:
        """Determine MariaDB dialect from version match"""
        version_parts = [int(x) for x in match.groups()[:2]]  # major.minor
        major, minor = version_parts[0], version_parts[1]
        
        if major == 11 and minor >= 4:
            return DialectType.MARIADB_11_4
        elif major == 11:
            return DialectType.MARIADB_11_0
        elif major == 10 and minor >= 6:
            return DialectType.MARIADB_10_6
        else:
            return DialectType.DEFAULT
    
    def _determine_galera_dialect(self, match: re.Match) -> DialectType:
        """Determine dialect from Galera version (less reliable)"""
        # Galera version alone is not definitive, return default
        return DialectType.DEFAULT
    
    def detect_from_content(self, content: str, max_lines: int = 100) -> DialectInfo:
        """
        Detect dialect from log content
        
        Args:
            content: Log file content or sample
            max_lines: Maximum number of lines to analyze for performance
            
        Returns:
            DialectInfo with detected dialect and confidence
        """
        lines = content.split('\n')[:max_lines]
        
        detection_scores = {}
        detection_methods = {}
        version_info = None
        features_found = []
        
        for line in lines:
            for category, pattern_list in self._detection_patterns.items():
                for pattern, dialect_or_func, confidence in pattern_list:
                    match = pattern.search(line)
                    if match:
                        if callable(dialect_or_func):
                            dialect = dialect_or_func(match)
                        else:
                            dialect = dialect_or_func
                            
                        if dialect not in detection_scores:
                            detection_scores[dialect] = 0
                            detection_methods[dialect] = []
                            
                        detection_scores[dialect] += confidence
                        detection_methods[dialect].append(f"{category}:{pattern.pattern[:50]}")
                        
                        if category == 'version_headers' and match.groups():
                            version_info = '.'.join(match.groups()[:3])
                            
                        if category == 'features':
                            features_found.append(f"{category}:{match.group(0)}")
            
            # Check MariaDB 10.6 specific patterns when no clear version detection
            if 'WSREP:' in line:
                # High confidence markers for MariaDB 10.6
                mariadb_10_6_markers = [
                    r'WSREP:\s+view\(view_id\(PRIM,',  # Very specific to MariaDB 10.6+ format
                    r'WSREP:\s+Node\s+[a-f0-9-]+\s+state\s+prim',
                    r'WSREP:\s+declaring\s+[a-f0-9-]+\s+at\s+tcp://',
                    r'WSREP:\s+connection\s+established\s+to\s+[a-f0-9-]+\s+tcp://',
                    r'WSREP:\s+forgetting\s+[a-f0-9-]+\s+\(tcp://',
                ]
                
                for pattern_str in mariadb_10_6_markers:
                    if re.search(pattern_str, line):
                        dialect = DialectType.MARIADB_10_6
                        if dialect not in detection_scores:
                            detection_scores[dialect] = 0
                            detection_methods[dialect] = []
                        detection_scores[dialect] += 0.4  # Strong confidence for these patterns
                        detection_methods[dialect].append(f"mariadb_10_6_pattern:{pattern_str[:30]}")
                        features_found.append(f"MariaDB_10_6_pattern:{line[:60]}")
                        break  # Only count once per line
        
        # Determine best dialect
        if detection_scores:
            best_dialect = max(detection_scores.keys(), key=lambda d: detection_scores[d])
            confidence = min(detection_scores[best_dialect], 1.0)
            method = '; '.join(detection_methods[best_dialect][:3])  # Top 3 methods
        else:
            best_dialect = DialectType.DEFAULT
            confidence = 0.1
            method = "no_detection_fallback"
        
        return DialectInfo(
            dialect_type=best_dialect,
            version=version_info or "unknown",
            confidence=confidence,
            detection_method=method,
            features=features_found
        )
    
    def detect_from_file(self, file_path: Path, sample_size: int = 10000) -> DialectInfo:
        """
        Detect dialect from log file
        
        Args:
            file_path: Path to log file
            sample_size: Number of characters to read for detection
            
        Returns:
            DialectInfo with detected dialect and confidence
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(sample_size)
            return self.detect_from_content(content)
        except Exception as e:
            self.logger.warning(f"Failed to detect dialect from {file_path}: {e}")
            return DialectInfo(
                dialect_type=DialectType.DEFAULT,
                version="unknown",
                confidence=0.1,
                detection_method=f"file_error:{str(e)[:50]}",
                features=[]
            )


class DialectPatternManager:
    """
    Manages dialect-specific patterns
    
    Provides pattern loading and fallback logic based on detected dialect
    """
    
    def __init__(self, pattern_dir: Path):
        self.pattern_dir = Path(pattern_dir)
        self.logger = logging.getLogger(__name__)
        self._pattern_cache = {}
        
    def get_pattern_files(self, dialect: DialectType) -> List[Path]:
        """Get pattern files for a specific dialect with fallback"""
        
        # Define pattern file priority order
        dialect_files = {
            DialectType.MARIADB_11_4: [
                'mariadb-11.4_patterns.yaml',
                'mariadb-11.0_patterns.yaml', 
                'view_patterns_mariadb_10_6.yaml',
                'mariadb-10.6_patterns.yaml',
                'view_patterns.yaml',
                'node_patterns.yaml',
                'sst_patterns.yaml'
            ],
            DialectType.MARIADB_11_0: [
                'mariadb-11.0_patterns.yaml',
                'view_patterns_mariadb_10_6.yaml',
                'mariadb-10.6_patterns.yaml', 
                'view_patterns.yaml',
                'node_patterns.yaml',
                'sst_patterns.yaml'
            ],
            DialectType.MARIADB_10_6: [
                'view_patterns_mariadb_10_6.yaml',  # Specific MariaDB 10.6 patterns first
                'node_patterns_mariadb_10_6.yaml',  # MariaDB 10.6 node patterns
                'mariadb-10.6_patterns.yaml',
                'view_patterns.yaml',
                'node_patterns.yaml', 
                'sst_patterns.yaml'
            ],
            DialectType.MYSQL_8_0: [
                'mysql-8.0_patterns.yaml',
                'view_patterns.yaml',
                'node_patterns.yaml',
                'sst_patterns.yaml'
            ],
            DialectType.PXC_8_0: [
                'pxc-8.0_patterns.yaml',
                'mysql-8.0_patterns.yaml',
                'view_patterns.yaml',
                'node_patterns.yaml',
                'sst_patterns.yaml'
            ],
            DialectType.DEFAULT: [
                'view_patterns.yaml',
                'node_patterns.yaml',
                'sst_patterns.yaml'
            ]
        }
        
        # Get potential files for this dialect
        potential_files = dialect_files.get(dialect, ['default_patterns.yaml'])
        
        # Find existing files with fallback
        existing_files = []
        for filename in potential_files:
            file_path = self.pattern_dir / filename
            if file_path.exists():
                existing_files.append(file_path)
        
        # If no dialect-specific files found, fall back to current pattern files
        if not existing_files:
            existing_files = list(self.pattern_dir.glob('*_patterns.yaml'))
            if not existing_files:
                existing_files = list(self.pattern_dir.glob('*.yaml'))
        
        self.logger.info(f"Using pattern files for {dialect.value}: {[f.name for f in existing_files]}")
        return existing_files
    
    def adapt_patterns_for_dialect(self, patterns: Dict[str, Any], dialect: DialectType) -> Dict[str, Any]:
        """Adapt pattern configurations based on dialect"""
        
        if dialect == DialectType.MARIADB_10_6:
            # Add MariaDB 10.6 specific adaptations
            patterns = self._add_mariadb_10_6_patterns(patterns)
        elif dialect == DialectType.MARIADB_11_4:
            # Add MariaDB 11.4 specific adaptations
            patterns = self._add_mariadb_11_4_patterns(patterns)
            
        return patterns
    
    def _add_mariadb_10_6_patterns(self, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Add MariaDB 10.6 specific VIEW and NODE patterns"""
        
        # Ensure VIEW patterns exist
        if 'VIEW' not in patterns:
            patterns['VIEW'] = []
            
        # Add actual MariaDB 10.6+ view format pattern
        mariadb_10_6_view_pattern = {
            'name': 'mariadb_view_format',
            'description': 'MariaDB 10.6+ view(view_id(...)) format',
            'confidence': 0.95,
            'regex': r'(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+\d+\s+\[(?P<level>\w+)\]\s+WSREP:\s+view\(view_id\((?P<cluster_state>PRIM|NON_PRIM),(?P<view_id>[^,]+),(?P<view_seq>\d+)\)\s+memb\s+\{',
            'field_mappings': {
                'cluster_state': 'cluster_state',
                'view_id': 'view_id', 
                'view_seq': 'view_seq'
            },
            'required_fields': ['cluster_state', 'view_id'],
            'examples': [
                '2025-09-19 18:17:16 0 [Note] WSREP: view(view_id(PRIM,4bff9935-9e34,42) memb {'
            ]
        }
        
        # Add member selection pattern
        member_selection_pattern = {
            'name': 'member_sst_selection',
            'description': 'Member SST donor selection',
            'confidence': 0.9,
            'regex': r'(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+\d+\s+\[(?P<level>\w+)\]\s+WSREP:\s+Member\s+(?P<joiner_index>\d+)\.(?P<joiner_id>\d+)\s+\((?P<joiner_name>[^)]+)\)\s+requested\s+state\s+transfer\s+from\s+.*Selected\s+(?P<donor_index>\d+)\.(?P<donor_id>\d+)\s+\((?P<donor_name>[^)]+)\)',
            'field_mappings': {
                'joiner_name': 'joiner_node',
                'donor_name': 'donor_node',
                'joiner_id': 'joiner_node_id',
                'donor_id': 'donor_node_id'
            },
            'required_fields': ['donor_node', 'joiner_node']
        }
        
        patterns['VIEW'].extend([mariadb_10_6_view_pattern, member_selection_pattern])
        
        return patterns
    
    def _add_mariadb_11_4_patterns(self, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Add MariaDB 11.4 specific patterns"""
        
        # MariaDB 11.4 has same view format as 10.6 but with additional features
        patterns = self._add_mariadb_10_6_patterns(patterns)
        
        # Add 11.4 specific patterns if needed
        if 'NODE' not in patterns:
            patterns['NODE'] = []
            
        # Add TLS service pattern (11.4 specific)
        tls_pattern = {
            'name': 'tls_service_init',
            'description': 'TLS service initialization (MariaDB 11.4+)',
            'confidence': 0.8,
            'regex': r'(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+\d+\s+\[(?P<level>\w+)\]\s+WSREP:\s+Initialization\s+of\s+the\s+TLS\s+service\s+for\s+WSREP\s+was\s+(?P<tls_status>skipped|enabled)',
            'field_mappings': {
                'tls_status': 'tls_enabled'
            },
            'required_fields': ['tls_enabled']
        }
        
        patterns['NODE'].append(tls_pattern)
        
        return patterns