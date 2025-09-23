#!/usr/bin/env python3
"""
SST Event Classifier for identifying SST lifecycle events.

This module provides classification logic to identify SST start, progress,
and end events from Galera log lines, enabling proper session management.
"""

import re
from typing import Optional, Dict, Any, List, Tuple
from enum import Enum


class SSTEventType(Enum):
    """Types of SST events in the lifecycle."""
    START = "SST_START"
    PROGRESS = "SST_PROGRESS" 
    END = "SST_END"
    ERROR = "SST_ERROR"
    UNKNOWN = "SST_UNKNOWN"


class SSTEventClassifier:
    """
    Classifies SST-related log lines into lifecycle events.
    
    This classifier uses pattern matching to identify different phases
    of SST operations: start, progress updates, completion, and errors.
    """
    
    # Patterns for SST start events - ONLY the actual SST request should create sessions
    START_PATTERNS = [
        # The ONLY pattern that should create sessions: actual SST request with node names
        (r'Member \d+\.\d+ \([^)]+\) requested state transfer from.*Selected \d+\.\d+ \([^)]+\)', re.IGNORECASE),
    ]
    
    # Patterns for SST progress events - All SST-related events except the initial request
    PROGRESS_PATTERNS = [
        # Former START patterns - these should update existing sessions, not create new ones
        (r'Running:.*wsrep_sst_mariabackup', re.IGNORECASE),  # "WSREP: Running: 'wsrep_sst_mariabackup"
        (r'mariabackup SST started', re.IGNORECASE),          # "WSREP_SST: [INFO] mariabackup SST started"
        (r'Streaming with mbstream', re.IGNORECASE),          # "WSREP_SST: [INFO] Streaming with mbstream"
        (r'Initiating SST/IST transfer', re.IGNORECASE),      # "WSREP: Initiating SST/IST transfer on DONOR side"
        (r'Requesting.*SST', re.IGNORECASE),
        (r'Starting.*SST.*to', re.IGNORECASE),
        (r'wsrep_sst_.*started', re.IGNORECASE),
        (r'State transfer required', re.IGNORECASE),
        (r'SST.*method.*selected', re.IGNORECASE),
        (r'Streaming.*the backup', re.IGNORECASE),
        
        # Common progress patterns across versions
        (r'Progress.*(\d+\.\d+)%', re.IGNORECASE),            # "Progress 45.5% transferred"
        (r'Sending.*(\d+).*bytes', re.IGNORECASE),            # Backup transfer progress
        (r'Transferred.*(\d+).*of.*(\d+)', re.IGNORECASE),    # Transfer status
        (r'backup.*progress.*(\d+)', re.IGNORECASE),          # Backup progress
        
        # MariaDB 11.4+ specific patterns (rate reporting)
        (r'Sending.*(\d+).*MB.*at.*rate.*(\d+\.\d+).*MB/s', re.IGNORECASE),  # 11.4+ rate pattern
        (r'SST.*rate.*(\d+\.\d+)', re.IGNORECASE),            # 11.4+ rate pattern
        
        # Enterprise log patterns
        (r'Waiting for SST streaming to complete', re.IGNORECASE),  # 11.4 streaming status
        (r'Disabling all progress/rate-limiting', re.IGNORECASE),   # 10.6 progress info
    ]
    
    # Patterns for SST completion/end events - Match the actual completion messages
    END_PATTERNS = [
        # The actual completion messages from the log
        (r'State transfer (to|from) \d+\.\d+ \([^)]+\) complete\.', re.IGNORECASE),  # "State transfer to/from 1.0 (NODE_50000) complete."
        (r'mariabackup SST completed on (joiner|donor)', re.IGNORECASE),              # "WSREP_SST: [INFO] mariabackup SST completed on joiner"
        
        # Legacy patterns for backward compatibility
        (r'SST complete', re.IGNORECASE),
        (r'SST.*finished', re.IGNORECASE),
        (r'SST.*completed.*successfully', re.IGNORECASE),
        (r'State.*transfer.*complete', re.IGNORECASE),
        (r'backup.*completed', re.IGNORECASE),
        (r'SST.*done', re.IGNORECASE),
        (r'wsrep_sst_.*completed', re.IGNORECASE),
    ]
    
    # Patterns for SST errors/failures (based on enterprise logs)
    ERROR_PATTERNS = [
        # Enterprise log error patterns
        (r'SST sending failed', re.IGNORECASE),               # "WSREP: SST sending failed: -32"
        (r'Process completed with error', re.IGNORECASE),     # "WSREP: Process completed with error"
        (r'Failed to read from:.*wsrep_sst', re.IGNORECASE),  # "WSREP: Failed to read from: wsrep_sst_mariabackup"
        (r'Command did not run:.*wsrep_sst', re.IGNORECASE),  # "WSREP: Command did not run: wsrep_sst_mariabackup"
        
        # Common error patterns
        (r'SST.*failed', re.IGNORECASE),
        (r'SST.*error', re.IGNORECASE),
        (r'SST.*abort', re.IGNORECASE),
        (r'State.*transfer.*failed', re.IGNORECASE),
        (r'backup.*failed', re.IGNORECASE),
        (r'SST.*cancelled', re.IGNORECASE),
        (r'wsrep_sst_.*failed', re.IGNORECASE),
        (r'SST.*timeout', re.IGNORECASE),
    ]
    
    def __init__(self):
        """Initialize the classifier with compiled patterns."""
        self.compiled_patterns = {
            SSTEventType.START: [re.compile(pattern, flags) for pattern, flags in self.START_PATTERNS],
            SSTEventType.PROGRESS: [re.compile(pattern, flags) for pattern, flags in self.PROGRESS_PATTERNS],
            SSTEventType.END: [re.compile(pattern, flags) for pattern, flags in self.END_PATTERNS],
            SSTEventType.ERROR: [re.compile(pattern, flags) for pattern, flags in self.ERROR_PATTERNS],
        }
    
    def classify_sst_event(self, log_line: str) -> SSTEventType:
        """
        Classify a log line as an SST event type.
        
        Args:
            log_line: The log line to classify
            
        Returns:
            SSTEventType: The classified event type
        """
        # Check error patterns first (highest priority)
        if self._matches_patterns(log_line, SSTEventType.ERROR):
            return SSTEventType.ERROR
            
        # Check end patterns 
        if self._matches_patterns(log_line, SSTEventType.END):
            return SSTEventType.END
            
        # Check start patterns
        if self._matches_patterns(log_line, SSTEventType.START):
            return SSTEventType.START
            
        # Check progress patterns
        if self._matches_patterns(log_line, SSTEventType.PROGRESS):
            return SSTEventType.PROGRESS
            
        return SSTEventType.UNKNOWN
    
    def _matches_patterns(self, log_line: str, event_type: SSTEventType) -> bool:
        """Check if log line matches any pattern for the given event type."""
        patterns = self.compiled_patterns.get(event_type, [])
        return any(pattern.search(log_line) for pattern in patterns)
    
    def extract_sst_details(self, log_line: str, event_type: SSTEventType) -> Dict[str, Any]:
        """
        Extract detailed information from an SST log line.
        
        Args:
            log_line: The SST log line
            event_type: The classified event type
            
        Returns:
            Dict with extracted SST details
        """
        details = {'event_type': event_type.value if hasattr(event_type, 'value') else str(event_type)}
        
        if event_type == SSTEventType.START:
            details.update(self._extract_start_details(log_line))
        elif event_type == SSTEventType.PROGRESS:
            details.update(self._extract_progress_details(log_line))
        elif event_type in [SSTEventType.END, SSTEventType.ERROR]:
            details.update(self._extract_end_details(log_line))
            
        return details
    
    def _extract_start_details(self, log_line: str) -> Dict[str, Any]:
        """Extract details from SST start events."""
        details = {}
        
        # Extract method information - the SST request line doesn't contain method info
        # Method will be extracted from subsequent PROGRESS events like "WSREP_SST: [INFO] mariabackup SST started"
        # So we don't set it here - let PROGRESS events update it
        
        # Extract donor/joiner information from SST request line
        # Pattern: "Member 1.0 (NODE_50000) requested state transfer from '*any*'. Selected 0.0 (NODE_54320)(SYNCED) as donor."
        sst_request_pattern = r'Member \d+\.\d+ \(([^)]+)\) requested state transfer from.*Selected \d+\.\d+ \(([^)]+)\)'
        match = re.search(sst_request_pattern, log_line, re.IGNORECASE)
        if match:
            details['joiner_node'] = match.group(1)  # NODE_50000
            details['donor_node'] = match.group(2)   # NODE_54320
        else:
            # Fallback to legacy patterns
            node_patterns = [
                r'to\s+([^\s,]+)',  # "SST to node123"
                r'from\s+([^\s,]+)',  # "SST from node456"
                r'donor\s*:\s*([^\s,]+)',  # "donor: node456"
                r'joiner\s*:\s*([^\s,]+)',  # "joiner: node123"
            ]
            
            for pattern in node_patterns:
                match = re.search(pattern, log_line, re.IGNORECASE)
                if match:
                    node_name = match.group(1)
                    if 'to' in pattern or 'joiner' in pattern:
                        details['joiner_node'] = node_name
                    elif 'from' in pattern or 'donor' in pattern:
                        details['donor_node'] = node_name
        
        details['transfer_status'] = 'started'
        return details
    
    def _extract_progress_details(self, log_line: str) -> Dict[str, Any]:
        """Extract details from SST progress events."""
        details = {}
        
        # Extract method from WSREP_SST script messages
        wsrep_sst_pattern = r'WSREP_SST:.*\[INFO\]\s+(\w+)\s+SST\s+(started|completed)'
        match = re.search(wsrep_sst_pattern, log_line, re.IGNORECASE)
        if match:
            details['transfer_method'] = match.group(1)  # mariabackup, rsync, etc.
        else:
            # Fallback method patterns for other progress messages
            method_patterns = [
                (r'method.*(?:rsync|mariabackup|xtrabackup|mysqldump)', re.IGNORECASE),
                (r'(?:rsync|mariabackup|xtrabackup|mysqldump).*method', re.IGNORECASE),
                (r'SST.*(?:rsync|mariabackup|xtrabackup|mysqldump)', re.IGNORECASE),
            ]
            
            for pattern in method_patterns:
                match = re.search(pattern[0], log_line, pattern[1])
                if match:
                    method_text = match.group(0).lower()
                    if 'mariabackup' in method_text:
                        details['transfer_method'] = 'mariabackup'
                    elif 'xtrabackup' in method_text:
                        details['transfer_method'] = 'xtrabackup'
                    elif 'rsync' in method_text:
                        details['transfer_method'] = 'rsync'
                    elif 'mysqldump' in method_text:
                        details['transfer_method'] = 'mysqldump'
                    break
        
        # Extract percentage
        percent_match = re.search(r'(\d+(?:\.\d+)?)%', log_line)
        if percent_match:
            details['progress_percentage'] = float(percent_match.group(1))
        
        # Extract bytes transferred
        bytes_patterns = [
            r'(\d+(?:\.\d+)?)\s*MB.*transferred',
            r'(\d+(?:\.\d+)?)\s*GB.*transferred', 
            r'Sending.*(\d+).*bytes',
            r'Transferred.*(\d+)',
        ]
        
        for pattern in bytes_patterns:
            match = re.search(pattern, log_line, re.IGNORECASE)
            if match:
                value = float(match.group(1))
                if 'MB' in pattern:
                    details['transferred_bytes'] = int(value * 1024 * 1024)
                elif 'GB' in pattern:
                    details['transferred_bytes'] = int(value * 1024 * 1024 * 1024)
                else:
                    details['transferred_bytes'] = int(value)
                break
        
        # Extract transfer rate
        rate_patterns = [
            r'rate.*(\d+(?:\.\d+)?)',
            r'(\d+(?:\.\d+)?)\s*MB/s',
            r'(\d+(?:\.\d+)?)\s*KB/s',
        ]
        
        for pattern in rate_patterns:
            match = re.search(pattern, log_line, re.IGNORECASE)
            if match:
                rate = float(match.group(1))
                if 'MB/s' in pattern:
                    details['transfer_rate'] = rate * 1024 * 1024  # bytes per second
                elif 'KB/s' in pattern:
                    details['transfer_rate'] = rate * 1024
                else:
                    details['transfer_rate'] = rate
                break
        
        details['transfer_status'] = 'in_progress'
        return details
    
    def _extract_end_details(self, log_line: str) -> Dict[str, Any]:
        """Extract details from SST end/error events."""
        details = {}
        
        # Determine final status
        if any(re.search(pattern, log_line, flags) for pattern, flags in self.ERROR_PATTERNS):
            details['transfer_status'] = 'failed'
            
            # Extract error information
            error_patterns = [
                r'error\s*:\s*(.+?)(?:\n|$)',
                r'failed\s*:\s*(.+?)(?:\n|$)', 
                r'ERROR\s*(.+?)(?:\n|$)',
            ]
            
            for pattern in error_patterns:
                match = re.search(pattern, log_line, re.IGNORECASE)
                if match:
                    details['error_message'] = match.group(1).strip()
                    break
        else:
            details['transfer_status'] = 'completed'
        
        # Extract final size if available
        size_patterns = [
            r'(\d+(?:\.\d+)?)\s*MB.*total',
            r'(\d+(?:\.\d+)?)\s*GB.*total',
            r'total.*(\d+).*bytes',
        ]
        
        for pattern in size_patterns:
            match = re.search(pattern, log_line, re.IGNORECASE)
            if match:
                value = float(match.group(1))
                if 'MB' in pattern:
                    details['total_bytes'] = int(value * 1024 * 1024)
                elif 'GB' in pattern:
                    details['total_bytes'] = int(value * 1024 * 1024 * 1024)
                else:
                    details['total_bytes'] = int(value)
                break
        
        return details
    
    def is_sst_related(self, log_line: str) -> bool:
        """Check if a log line is related to SST operations."""
        sst_keywords = ['sst', 'state transfer', 'backup', 'wsrep_sst']
        line_lower = log_line.lower()
        return any(keyword in line_lower for keyword in sst_keywords)