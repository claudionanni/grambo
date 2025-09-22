#!/usr/bin/env python3
"""
Temporal property system for tracking property changes over time.

This module provides the TemporalProperty class which stores property values
with timestamps, enabling timeline reconstruction and temporal queries.
"""

from datetime import datetime
from typing import List, Tuple, Any, Optional


class TemporalProperty:
    """
    A property that tracks its values over time.
    
    Stores a timeline of (timestamp, value) pairs and provides methods
    to query values at specific points in time.
    """
    
    def __init__(self, initial_value: Any = None, initial_timestamp: Optional[datetime] = None):
        """
        Initialize temporal property.
        
        Args:
            initial_value: Initial value for the property
            initial_timestamp: Timestamp for initial value (defaults to now)
        """
        self.timeline: List[Tuple[datetime, Any]] = []
        
        if initial_value is not None:
            if initial_timestamp is None:
                initial_timestamp = datetime.now()
            self.timeline.append((initial_timestamp, initial_value))
    
    def update(self, value: Any, timestamp: Optional[datetime] = None) -> None:
        """
        Add a new value to the timeline.
        
        Args:
            value: New value for the property
            timestamp: When the value changed (defaults to now)
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        self.timeline.append((timestamp, value))
        # Keep timeline sorted by timestamp
        self.timeline.sort(key=lambda x: x[0])
    
    @property
    def current_value(self) -> Any:
        """Get the most recent value."""
        return self.timeline[-1][1] if self.timeline else None
    
    @property
    def initial_value(self) -> Any:
        """Get the first value."""
        return self.timeline[0][1] if self.timeline else None
    
    def value_at(self, timestamp: datetime) -> Any:
        """
        Get the value that was active at a specific timestamp.
        
        Args:
            timestamp: The point in time to query
            
        Returns:
            The value that was active at that time, or None if no value exists
        """
        for ts, value in reversed(self.timeline):
            if ts <= timestamp:
                return value
        return None
    
    def changes_between(self, start_time: datetime, end_time: datetime) -> List[Tuple[datetime, Any]]:
        """
        Get all changes that occurred between two timestamps.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            List of (timestamp, value) tuples for changes in the range
        """
        changes = []
        for timestamp, value in self.timeline:
            if start_time <= timestamp <= end_time:
                changes.append((timestamp, value))
        return changes
    
    def has_changed(self) -> bool:
        """Check if the property has changed from its initial value."""
        return len(self.timeline) > 1
    
    def change_count(self) -> int:
        """Get the number of times this property has changed."""
        return len(self.timeline)
    
    def get_timeline(self) -> List[Tuple[datetime, Any]]:
        """Get the complete timeline of changes."""
        return self.timeline.copy()
    
    def __str__(self) -> str:
        """String representation showing current value and change count."""
        current = self.current_value
        count = self.change_count()
        return f"TemporalProperty(current={current}, changes={count})"
    
    def __repr__(self) -> str:
        return self.__str__()


class TemporalPropertySet:
    """
    A collection of temporal properties for an entity.
    
    Provides convenience methods to manage multiple temporal properties
    and query their states at specific points in time.
    """
    
    def __init__(self):
        """Initialize empty property set."""
        self.properties: dict[str, TemporalProperty] = {}
    
    def add_property(self, name: str, initial_value: Any = None, 
                    initial_timestamp: Optional[datetime] = None) -> TemporalProperty:
        """
        Add a new temporal property.
        
        Args:
            name: Property name
            initial_value: Initial value
            initial_timestamp: Initial timestamp
            
        Returns:
            The created TemporalProperty
        """
        prop = TemporalProperty(initial_value, initial_timestamp)
        self.properties[name] = prop
        return prop
    
    def update_property(self, name: str, value: Any, timestamp: Optional[datetime] = None) -> None:
        """
        Update a property value.
        
        Args:
            name: Property name
            value: New value
            timestamp: When the change occurred
        """
        if name not in self.properties:
            self.add_property(name, value, timestamp)
        else:
            self.properties[name].update(value, timestamp)
    
    def get_property(self, name: str) -> Optional[TemporalProperty]:
        """Get a temporal property by name."""
        return self.properties.get(name)
    
    def get_current_values(self) -> dict[str, Any]:
        """Get current values of all properties."""
        return {name: prop.current_value for name, prop in self.properties.items()}
    
    def get_values_at(self, timestamp: datetime) -> dict[str, Any]:
        """Get values of all properties at a specific timestamp."""
        return {name: prop.value_at(timestamp) for name, prop in self.properties.items()}
    
    def get_property_names(self) -> List[str]:
        """Get names of all properties."""
        return list(self.properties.keys())
    
    def has_property(self, name: str) -> bool:
        """Check if a property exists."""
        return name in self.properties