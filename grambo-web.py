#!/usr/bin/env python3
"""
Grambo Web Visualizer
=====================

Interactive web-based visualization of Galera cluster state changes over time.
Uses Plotly/Dash for modern, professional cluster analysis interface.

Usage:
    python3 grambo-web.py cluster-analysis.json
    python3 grambo-web.py --port 8080 cluster-analysis.json
    python3 grambo-web.py --host 0.0.0.0 --port 8080 cluster-analysis.json

Features:
    - Interactive network diagram with node states
    - Timeline slider for navigation through cluster states  
    - Animated SST/IST transfer visualization
    - Event log with filtering a                       )
            
            if state.transfers:   )
            
            if state.transfers:   )
            
            if state.transfers:   )
            
            if state.transfers:    )
            
            if state.transfers:                   
            if state.transfers:      }
            
            if state.transfers:    
            if state.transfers:   
            if state.transfers:t capabilities (PNG, SVG, PDF)
    - Real-time playback controls
"""

import sys
import json
import argparse
import math
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple
import webbrowser
import threading
import time

try:
    import dash
    from dash import dcc, html, Input, Output, State, callback
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    import pandas as pd
    import networkx as nx
except ImportError as e:
    print(f"Error: Required libraries not found: {e}")
    print("Install with: pip install dash plotly pandas networkx")
    sys.exit(1)

class ClusterState:
    """Represents cluster state at a specific point in time"""
    def __init__(self, timestamp: datetime, frame_id: int):
        self.timestamp = timestamp
        self.frame_id = frame_id
        self.nodes: Dict[str, Dict] = {}  # node_name -> {state, seqno, issues, etc}
        self.transfers: List[Dict] = []   # active transfers
        self.events: List[str] = []       # events in this frame
        self.issues: List[str] = []       # cluster issues
    
    def add_node(self, name: str, state: str, **kwargs):
        """Add or update a node in this cluster state"""
        self.nodes[name] = {
            'state': state,
            'seqno': kwargs.get('seqno'),
            'issues': kwargs.get('issues', []),
            'sst_status': kwargs.get('sst_status'),
            'ist_status': kwargs.get('ist_status')
        }
    
    def add_transfer(self, transfer_type: str, joiner: str, donor: str, status: str, method: str = 'unknown'):
        """Add an active transfer to this state"""
        self.transfers.append({
            'type': transfer_type,
            'joiner': joiner,
            'donor': donor,
            'status': status,
            'method': method
        })

class WebClusterVisualizer:
    """Web-based cluster visualizer using Plotly/Dash"""
    
    def __init__(self, cluster_data: Dict[str, Any], host: str = '127.0.0.1', port: int = 8050):
        self.cluster_data = cluster_data
        self.host = host
        self.port = port
        self.states: List[ClusterState] = []
        self.app = dash.Dash(__name__)
        self.current_state_index = 0
        
        # Parse cluster data into states
        self.parse_cluster_data()
        
        # Setup the app
        self.setup_layout()
        self.setup_callbacks()
    
    def parse_cluster_data(self):
        """Parse cluster analysis JSON into timeline states"""
        analysis = self.cluster_data.get('cluster_analysis', {})
        
        # Create mapping from file node IDs to actual Galera node names
        node_name_mapping = {}
        
        # Extract real node names from SST workflows
        for workflow in analysis.get('sst_workflows', []):
            joiner = workflow.get('joiner')
            donor = workflow.get('donor')
            
            # Map based on known NODE patterns to file IDs
            if joiner == 'NODE_50000':
                node_name_mapping['00'] = joiner
            elif joiner == 'NODE_54320':  
                node_name_mapping['20'] = joiner
            elif joiner == 'NODE_54321':
                node_name_mapping['21'] = joiner
                
            if donor == 'NODE_50000':
                node_name_mapping['00'] = donor
            elif donor == 'NODE_54320':
                node_name_mapping['20'] = donor  
            elif donor == 'NODE_54321':
                node_name_mapping['21'] = donor
        
        # Fallback mapping if not found in workflows (use known mappings)
        if '00' not in node_name_mapping:
            node_name_mapping['00'] = 'NODE_50000'
        if '20' not in node_name_mapping:
            node_name_mapping['20'] = 'NODE_54320'
        if '21' not in node_name_mapping:
            node_name_mapping['21'] = 'NODE_54321'
            
        # Also handle legacy db1/db3 analysis mappings for backward compatibility
        if 'db1-analysis' not in node_name_mapping:
            node_name_mapping['db1-analysis'] = 'UAT-DB-01'
        if 'db3-analysis' not in node_name_mapping:
            node_name_mapping['db3-analysis'] = 'UAT-DB-03'
        
        # Get all events from state transitions, SST workflows, and cluster views
        all_events = []
        
        # Add state transitions as individual events
        for transition in analysis.get('state_transitions', []):
            timestamp_str = transition.get('timestamp', '')
            if timestamp_str:
                timestamp = self.parse_timestamp(timestamp_str)
                if timestamp:
                    all_events.append({
                        'timestamp': timestamp,
                        'type': 'state_transition',
                        'node': transition.get('node'),
                        'data': transition
                    })
        
        # Add SST workflows as events
        for workflow in analysis.get('sst_workflows', []):
            timestamp_str = workflow.get('request_time', '')
            if timestamp_str:
                timestamp = self.parse_timestamp(timestamp_str)
                if timestamp:
                    all_events.append({
                        'timestamp': timestamp,
                        'type': 'sst_workflow',
                        'node': workflow.get('joiner'),
                        'data': workflow
                    })
        
        # Sort events by timestamp
        all_events.sort(key=lambda x: x['timestamp'])
        
        # Get node list and apply mapping
        nodes = analysis.get('nodes', [])
        mapped_nodes = [node_name_mapping.get(node, node) for node in nodes]
        
        if not all_events:
            # Create a default state if no events
            default_state = ClusterState(datetime.now(), 0)
            for node in mapped_nodes:
                default_state.add_node(node, 'SYNCED')
            default_state.events.append('Cluster operational')
            self.states.append(default_state)
            return
        
        # Initialize node states - determine initial states from first events
        current_node_states = {}
        
        # First, try to determine initial states from the first few state transitions
        initial_states_found = {}
        for event in all_events[:10]:  # Check first 10 events for initial states
            if event['type'] == 'state_transition':
                transition = event['data']
                original_node = transition.get('node')
                mapped_node = node_name_mapping.get(original_node, original_node)
                from_state = transition.get('from_state')
                if mapped_node and from_state and mapped_node not in initial_states_found:
                    initial_states_found[mapped_node] = from_state
        
        # Initialize nodes with discovered initial states or reasonable defaults
        for node in mapped_nodes:
            if node in initial_states_found:
                current_node_states[node] = initial_states_found[node]
            else:
                # Default to PRIMARY for nodes without state transitions (stable nodes)
                current_node_states[node] = 'PRIMARY'
        
        # Build timeline of states - create one frame per significant event
        state_id = 0
        
        # Create initial state
        initial_state = ClusterState(all_events[0]['timestamp'], state_id)
        for node_name, state in current_node_states.items():
            initial_state.add_node(node_name, state)
        initial_state.events.append('Initial cluster state')
        self.states.append(initial_state)
        state_id += 1
        
        for event in all_events:
            # Create new state for each event
            new_state = ClusterState(event['timestamp'], state_id)
            
            # Copy previous state
            for node_name, state in current_node_states.items():
                new_state.add_node(node_name, state)
            
            # Apply event changes
            if event['type'] == 'state_transition':
                transition = event['data']
                original_node = transition.get('node')
                mapped_node = node_name_mapping.get(original_node, original_node)
                from_state = transition.get('from_state')
                to_state = transition.get('to_state')
                seqno = transition.get('seqno')
                
                if mapped_node and to_state:
                    # Update node state
                    current_node_states[mapped_node] = to_state
                    new_state.nodes[mapped_node]['state'] = to_state
                    new_state.nodes[mapped_node]['seqno'] = seqno
                
                new_state.events.append(f"{mapped_node}: {from_state} → {to_state}" + (f" (seqno: {seqno})" if seqno else ""))
                
                # Add special handling for JOINER state (implies SST in progress)
                if to_state == 'JOINER':
                    new_state.nodes[mapped_node]['sst_status'] = 'receiving'
                    # Try to find the donor (usually the PRIMARY node)
                    for other_node, other_state in current_node_states.items():
                        if other_node != mapped_node and other_state in ['PRIMARY', 'SYNCED']:
                            new_state.add_transfer('SST', mapped_node, other_node, 'in_progress', 'rsync')
                            break
                elif from_state == 'JOINER' and to_state in ['SYNCED', 'PRIMARY']:
                    new_state.nodes[mapped_node]['sst_status'] = None
            
            elif event['type'] == 'sst_workflow':
                workflow = event['data']
                joiner = workflow.get('joiner')
                donor = workflow.get('donor') 
                status = workflow.get('status', 'unknown')
                method = workflow.get('method', 'unknown')
                
                # Use the actual Galera node names directly
                joiner_node = joiner
                donor_node = donor
                
                if joiner_node and joiner_node in current_node_states:
                    if status == 'requested':
                        new_state.events.append(f"SST requested: {joiner_node} requesting from {donor_node or 'cluster'}")
                    elif status == 'started':
                        current_node_states[joiner_node] = 'JOINER'
                        new_state.nodes[joiner_node]['state'] = 'JOINER'
                        new_state.nodes[joiner_node]['sst_status'] = 'receiving'
                        
                        if donor_node and donor_node in current_node_states:
                            current_node_states[donor_node] = 'DONOR'
                            new_state.nodes[donor_node]['state'] = 'DONOR'
                            new_state.nodes[donor_node]['sst_status'] = 'sending'
                        
                        new_state.add_transfer('SST', joiner_node, donor_node or 'unknown', 'started', method)
                        new_state.events.append(f"SST started: {joiner_node} ← {donor_node or 'unknown'} ({method})")
                    
                    elif status == 'completed':
                        if joiner_node in current_node_states:
                            current_node_states[joiner_node] = 'SYNCED'
                            new_state.nodes[joiner_node]['state'] = 'SYNCED'
                            new_state.nodes[joiner_node]['sst_status'] = None
                        
                        if donor_node and donor_node in current_node_states:
                            current_node_states[donor_node] = 'SYNCED'
                            new_state.nodes[donor_node]['state'] = 'SYNCED'
                            new_state.nodes[donor_node]['sst_status'] = None
                        
                        new_state.events.append(f"SST completed: {joiner_node} now synchronized")
                    
                    elif status == 'failed':
                        if joiner_node in current_node_states:
                            new_state.nodes[joiner_node]['issues'] = ['SST failed']
                        new_state.issues.append("SST failure")
                        new_state.events.append(f"SST failed: {joiner_node} failed to synchronize")
            
            self.states.append(new_state)
            state_id += 1
        
        print(f"✓ Created {len(self.states)} cluster states from {len(all_events)} events")
        print(f"  - State transitions: {len([e for e in all_events if e['type'] == 'state_transition'])}")
        print(f"  - SST workflows: {len([e for e in all_events if e['type'] == 'sst_workflow'])}")
    
    def parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse timestamp from various formats"""
        if not timestamp_str:
            return None
            
        formats = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%m-%d %H:%M:%S',
            '%H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def create_network_graph(self, state: ClusterState) -> go.Figure:
        """Create network diagram for the current state"""
        fig = go.Figure()
        
        # Calculate node positions in a circle
        nodes = list(state.nodes.keys())
        n_nodes = len(nodes)
        
        if n_nodes == 0:
            return fig
        
        # Identify nodes that are part of the established cluster vs uncertain nodes
        established_nodes = []
        uncertain_nodes = []
        
        for node, node_data in state.nodes.items():
            node_state = node_data['state']
            # Consider a node uncertain if it's in transitional states or problematic states
            if node_state in ['JOINER', 'JOINING', 'OPEN', 'CLOSED', 'DESYNCED', 'UNKNOWN']:
                uncertain_nodes.append(node)
            else:
                established_nodes.append(node)
        
        # Position established nodes in a circle
        positions = {}
        
        # Position established nodes in main circle
        for i, node in enumerate(established_nodes):
            angle = 2 * math.pi * i / max(len(established_nodes), 1)
            x = math.cos(angle)
            y = math.sin(angle)
            positions[node] = (x, y)
        
        # Position uncertain nodes outside the circle (no connections)
        for i, node in enumerate(uncertain_nodes):
            angle = 2 * math.pi * i / max(len(uncertain_nodes), 1)
            # Place them further out and offset
            x = 1.8 * math.cos(angle)
            y = 1.8 * math.sin(angle)
            positions[node] = (x, y)
        
        # Add edges ONLY between established nodes (exclude uncertain nodes)
        edge_x, edge_y = [], []
        for i, node1 in enumerate(established_nodes):
            for j, node2 in enumerate(established_nodes[i+1:], i+1):
                x1, y1 = positions[node1]
                x2, y2 = positions[node2]
                edge_x.extend([x1, x2, None])
                edge_y.extend([y1, y2, None])
        
        # Add edges to plot
        if edge_x:  # Only add edges if there are established nodes
            fig.add_trace(go.Scatter(
                x=edge_x, y=edge_y,
                line=dict(width=2, color='lightgray'),
                hoverinfo='none',
                mode='lines',
                name='connections'
            ))
        
        # Add nodes
        node_x, node_y, node_colors, node_text, hover_text = [], [], [], [], []
        
        for node, (x, y) in positions.items():
            node_data = state.nodes[node]
            node_state = node_data['state']
            
            # Color based on state - consistent with status display
            color_map = {
                # Node sync states (established cluster members)
                'SYNCED': 'green',
                'DONOR': 'blue', 
                'DONOR/DESYNCED': 'blue',
                'JOINED': 'yellow',
                'CONNECTED': 'lightblue',
                'PRIMARY': 'darkgreen',
                'NON_PRIMARY': 'lightcoral',
                # Uncertain/transitional states
                'JOINER': 'orange',
                'JOINING': 'orange', 
                'DESYNCED': 'orange',
                'OPEN': 'lightcoral',
                'CLOSED': 'gray',
                # Unknown/problematic
                'UNKNOWN': 'lightgray'
            }
            color = color_map.get(node_state, 'gray')
            
            # Add issues indicator
            if node_data.get('issues'):
                color = 'darkred'
            
            # Special styling for uncertain nodes
            is_uncertain = node_state in ['JOINER', 'JOINING', 'OPEN', 'CLOSED', 'DESYNCED', 'UNKNOWN']
            
            node_x.append(x)
            node_y.append(y)
            node_colors.append(color)
            
            # Different node text styling for uncertain nodes
            if is_uncertain:
                node_text.append(f"{node}?")  # Add question mark to uncertain nodes
            else:
                node_text.append(node)
            
            # Hover text with details
            hover_info = f"<b>{node}</b><br>"
            hover_info += f"State: {node_state}<br>"
            if is_uncertain:
                hover_info += "<b>Status: Not fully joined to cluster</b><br>"
            if node_data.get('seqno'):
                hover_info += f"Seqno: {node_data['seqno']}<br>"
            if node_data.get('sst_status'):
                hover_info += f"SST: {node_data['sst_status']}<br>"
            if node_data.get('ist_status'):
                hover_info += f"IST: {node_data['ist_status']}<br>"
            if node_data.get('issues'):
                hover_info += f"Issues: {', '.join(node_data['issues'])}"
            
            hover_text.append(hover_info)
        
        # Add nodes to plot
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            marker=dict(
                size=80,
                color=node_colors,
                line=dict(width=3, color='white')
            ),
            text=node_text,
            textposition="middle center",
            textfont=dict(size=14, color="white", family="Arial Black"),
            hovertext=hover_text,
            hoverinfo='text',
            name='nodes'
        ))
        
        # Add transfer arrows
        for transfer in state.transfers:
            joiner = transfer.get('joiner')
            donor = transfer.get('donor')
            
            if joiner in positions and donor in positions:
                x1, y1 = positions[donor]
                x2, y2 = positions[joiner]
                
                # Arrow color based on transfer status
                status = transfer.get('status', 'unknown')
                arrow_color = {
                    'started': 'orange',
                    'completed': 'green',
                    'failed': 'red'
                }.get(status, 'blue')
                
                # Add arrow annotation
                fig.add_annotation(
                    x=x2, y=y2,
                    ax=x1, ay=y1,
                    xref='x', yref='y',
                    axref='x', ayref='y',
                    arrowhead=2,
                    arrowsize=2,
                    arrowwidth=3,
                    arrowcolor=arrow_color,
                    text=f"{transfer.get('type', 'Transfer')}",
                    textangle=0,
                    font=dict(size=10, color=arrow_color)
                )
        
        # Update layout
        fig.update_layout(
            title={
                'text': f"Cluster State - Frame {state.frame_id + 1}",
                'font': {'size': 16}
            },
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='rgba(0,0,0,0)',
            height=500
        )
        
        return fig
    
    def setup_layout(self):
        """Setup the Dash app layout"""
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1("🎬 Grambo Cluster Visualizer", 
                       style={'margin': '0', 'color': '#2c3e50'}),
                html.P(f"Interactive visualization of {len(self.states)} cluster states",
                       style={'margin': '5px 0', 'color': '#7f8c8d'})
            ], style={'textAlign': 'center', 'padding': '20px', 'backgroundColor': '#ecf0f1'}),
            
            # Main content
            html.Div([
                # Left panel: Network diagram
                html.Div([
                    dcc.Graph(
                        id='cluster-network',
                        style={'height': '500px'}
                    ),
                    
                    # Timeline controls
                    html.Div([
                        html.Label("Timeline Navigation:", style={'fontWeight': 'bold'}),
                        dcc.Slider(
                            id='timeline-slider',
                            min=0,
                            max=len(self.states) - 1,
                            value=0,
                            marks={i: f"Frame {i+1}" for i in range(0, len(self.states), max(1, len(self.states)//10))},
                            step=1,
                            tooltip={"placement": "bottom", "always_visible": True}
                        )
                    ], style={'padding': '20px'}),
                    
                    # Playback controls
                    html.Div([
                        html.Button('⏮️', id='first-btn', n_clicks=0, style={'margin': '5px'}, title='First Frame'),
                        html.Button('⏪', id='prev-btn', n_clicks=0, style={'margin': '5px'}, title='Previous Frame'),
                        html.Button('⏯️', id='play-btn', n_clicks=0, style={'margin': '5px'}, title='Play/Pause'),
                        html.Button('⏩', id='next-btn', n_clicks=0, style={'margin': '5px'}, title='Next Frame'),
                        html.Button('⏭️', id='last-btn', n_clicks=0, style={'margin': '5px'}, title='Last Frame'),
                        html.Span('Speed: ', style={'margin-left': '20px'}),
                        dcc.Dropdown(
                            id='speed-dropdown',
                            options=[
                                {'label': '0.5x', 'value': 2000},
                                {'label': '1x', 'value': 1000},
                                {'label': '2x', 'value': 500},
                                {'label': '5x', 'value': 200}
                            ],
                            value=1000,
                            style={'width': '80px', 'display': 'inline-block'}
                        )
                    ], style={'textAlign': 'center', 'padding': '10px'})
                    
                ], style={'width': '60%', 'display': 'inline-block', 'verticalAlign': 'top'}),
                
                # Right panel: Event log and details
                html.Div([
                    html.H3("📋 Current State", style={'color': '#2c3e50'}),
                    html.Div(id='state-details'),
                    
                    html.H3("📜 Event Log", style={'color': '#2c3e50', 'marginTop': '30px'}),
                    html.Div(id='event-log', style={
                        'height': '300px', 
                        'overflowY': 'scroll',
                        'border': '1px solid #bdc3c7',
                        'padding': '10px',
                        'backgroundColor': '#f9f9f9'
                    })
                    
                ], style={'width': '38%', 'display': 'inline-block', 'verticalAlign': 'top', 'padding': '20px'})
                
            ], style={'display': 'flex'}),
            
            # Auto-play interval
            dcc.Interval(
                id='auto-play-interval',
                interval=1000,  # 1 second
                n_intervals=0,
                disabled=True
            ),
            
            # Hidden div to store play state
            html.Div(id='play-state', style={'display': 'none'}, children='paused')
            
        ], style={'fontFamily': 'Arial, sans-serif'})
    
    def setup_callbacks(self):
        """Setup Dash callbacks for interactivity"""
        
        @self.app.callback(
            [Output('cluster-network', 'figure'),
             Output('state-details', 'children'),
             Output('event-log', 'children')],
            [Input('timeline-slider', 'value')]
        )
        def update_visualization(frame_index):
            if frame_index is None or frame_index >= len(self.states):
                frame_index = 0
            
            state = self.states[frame_index]
            
            # Update network graph
            network_fig = self.create_network_graph(state)
            
            # Update state details
            details = [
                html.H4(f"Frame {state.frame_id + 1} of {len(self.states)}", style={'margin': '10px 0', 'color': '#2c3e50'}),
                html.P(f"⏱️ Time: {state.timestamp.strftime('%Y-%m-%d %H:%M:%S')}", style={'margin': '5px 0'}),
                html.P(f"�️ Nodes: {len(state.nodes)}", style={'margin': '5px 0'})
            ]
            
            # Add individual node states
            color_map = {
                # Node sync states (established cluster members)
                'SYNCED': 'green',
                'DONOR': 'blue', 
                'DONOR/DESYNCED': 'blue',
                'JOINED': 'yellow',
                'CONNECTED': 'lightblue',
                'PRIMARY': 'darkgreen',
                'NON_PRIMARY': 'lightcoral',
                # Uncertain/transitional states
                'JOINER': 'orange',
                'JOINING': 'orange', 
                'DESYNCED': 'orange',
                'OPEN': 'lightcoral',
                'CLOSED': 'gray',
                # Unknown/problematic
                'UNKNOWN': 'lightgray'
            }
            
            # Separate established vs uncertain nodes in display
            established_nodes = []
            uncertain_nodes = []
            
            for node_name, node_data in state.nodes.items():
                node_state = node_data['state']
                if node_state in ['JOINER', 'JOINING', 'OPEN', 'CLOSED', 'DESYNCED', 'UNKNOWN']:
                    uncertain_nodes.append((node_name, node_state))
                else:
                    established_nodes.append((node_name, node_state))
            
            # Display established cluster members first
            if established_nodes:
                details.append(html.P("🔗 Cluster Members:", style={'margin': '8px 0 2px 0', 'fontWeight': 'bold', 'fontSize': '15px'}))
                for node_name, node_state in established_nodes:
                    color = color_map.get(node_state, 'gray')
                    details.append(
                        html.P(f"  {node_name}: {node_state}", 
                              style={'margin': '2px 0', 'fontSize': '14px', 'color': color, 'marginLeft': '10px'})
                    )
            
            # Display uncertain/joining nodes separately  
            if uncertain_nodes:
                details.append(html.P("⚠️ Uncertain Nodes:", style={'margin': '8px 0 2px 0', 'fontWeight': 'bold', 'fontSize': '15px', 'color': 'orange'}))
                for node_name, node_state in uncertain_nodes:
                    color = color_map.get(node_state, 'gray')
                    details.append(
                        html.P(f"  {node_name}: {node_state} (not fully joined)", 
                              style={'margin': '2px 0', 'fontSize': '14px', 'color': color, 'marginLeft': '10px', 'fontStyle': 'italic'})
                    )
            
            if state.transfers:
                details.append(html.P(f"📡 Active Transfers: {len(state.transfers)}", style={'margin': '5px 0', 'color': 'orange'}))
            
            if state.issues:
                details.append(html.P(f"⚠️ Issues: {len(state.issues)}", style={'margin': '5px 0', 'color': 'red'}))
            
            # Update event log
            event_items = []
            for event in state.events:
                event_items.append(html.P(f"• {event}", style={'margin': '2px 0', 'fontSize': '14px'}))
            
            if state.issues:
                for issue in state.issues:
                    event_items.append(html.P(f"⚠️ {issue}", style={'margin': '2px 0', 'fontSize': '14px', 'color': 'red'}))
            
            return network_fig, details, event_items
        
        @self.app.callback(
            [Output('timeline-slider', 'value'),
             Output('play-state', 'children')],
            [Input('first-btn', 'n_clicks'),
             Input('prev-btn', 'n_clicks'),
             Input('play-btn', 'n_clicks'),
             Input('next-btn', 'n_clicks'),
             Input('last-btn', 'n_clicks'),
             Input('auto-play-interval', 'n_intervals')],
            [State('timeline-slider', 'value'),
             State('play-state', 'children')]
        )
        def handle_playback_controls(first_clicks, prev_clicks, play_clicks, next_clicks, last_clicks, n_intervals, current_frame, play_state):
            ctx = dash.callback_context
            
            if not ctx.triggered:
                return current_frame, play_state
            
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            
            if button_id == 'first-btn':
                return 0, play_state
            elif button_id == 'prev-btn':
                new_frame = max(0, (current_frame or 0) - 1)
                return new_frame, play_state
            elif button_id == 'next-btn':
                new_frame = min(len(self.states) - 1, (current_frame or 0) + 1)
                return new_frame, play_state
            elif button_id == 'last-btn':
                return len(self.states) - 1, play_state
            elif button_id == 'play-btn':
                new_state = 'playing' if play_state == 'paused' else 'paused'
                return current_frame, new_state
            elif button_id == 'auto-play-interval' and play_state == 'playing':
                next_frame = (current_frame + 1) % len(self.states)
                return next_frame, play_state
            
            return current_frame, play_state
        
        @self.app.callback(
            [Output('auto-play-interval', 'disabled'),
             Output('auto-play-interval', 'interval')],
            [Input('play-state', 'children'),
             Input('speed-dropdown', 'value')]
        )
        def update_auto_play(play_state, speed):
            disabled = play_state == 'paused'
            return disabled, speed
    
    def run(self, debug: bool = False, open_browser: bool = True):
        """Run the web application"""
        url = f"http://{self.host}:{self.port}"
        
        if open_browser:
            # Open browser after a short delay
            def open_browser_delayed():
                time.sleep(1.5)
                webbrowser.open(url)
            
            browser_thread = threading.Thread(target=open_browser_delayed)
            browser_thread.daemon = True
            browser_thread.start()
        
        print(f"🌐 Starting Grambo Web Visualizer...")
        print(f"🔗 Open your browser to: {url}")
        print(f"📊 Loaded {len(self.states)} cluster states")
        print(f"⏹️  Press Ctrl+C to stop")
        
        self.app.run(host=self.host, port=self.port, debug=debug)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Grambo Web Visualizer - Interactive cluster analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s cluster-analysis.json
  %(prog)s --port 8080 cluster-analysis.json  
  %(prog)s --host 0.0.0.0 --port 8080 cluster-analysis.json
  %(prog)s --no-browser cluster-analysis.json
        """
    )
    
    parser.add_argument('cluster_file', 
                       help='JSON file from grambo-cluster.py output')
    
    parser.add_argument('--host', default='127.0.0.1',
                       help='Host to bind web server (default: 127.0.0.1)')
    
    parser.add_argument('--port', type=int, default=8050,
                       help='Port for web server (default: 8050)')
    
    parser.add_argument('--no-browser', action='store_true',
                       help='Do not automatically open browser')
    
    parser.add_argument('--debug', action='store_true',
                       help='Run in debug mode with auto-reload')
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    try:
        with open(args.cluster_file, 'r') as f:
            cluster_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading cluster file: {e}", file=sys.stderr)
        sys.exit(1)
    
    visualizer = WebClusterVisualizer(
        cluster_data, 
        host=args.host, 
        port=args.port
    )
    
    try:
        visualizer.run(
            debug=args.debug,
            open_browser=not args.no_browser
        )
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    except Exception as e:
        print(f"Error starting web server: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
