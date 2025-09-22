#!/usr/bin/env python3
"""
GRAP - Galera Regex Analysis Parser (Next Generation)
Entity-based log parsing system for MariaDB/Galera cluster analysis

This is the new grambo architecture implementation based on entity extraction
rather than regex pattern matching. It provides interactive learning capabilities
and versioned pattern registries.

Usage:
    grap.py [options] <logfile>
    grap.py --help
    grap.py --version

Examples:
    # Basic analysis with default output
    grap.py galera.log
    
    # JSON output for integration
    grap.py --format=json galera.log
    
    # Filter specific entity types
    grap.py --entities=NODE,STATE_TRANSFER galera.log
    
    # Interactive learning mode
    grap.py --learn galera.log
    
    # Use specific pattern version
    grap.py --pattern-version=10.6.8 galera.log
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import List, Optional

# Version information
__version__ = "2.0.0-alpha1"
__author__ = "Claudio Nanni"
__description__ = "Galera Regex Analysis Parser - Entity-based log analysis"

# Import core modules (to be implemented)
try:
    from lib.parser import LogParser
    from lib.output.formatter import OutputFormatter
    from lib.entities.registry import EntityRegistry
    from lib.patterns.matcher import PatternMatcher
except ImportError as e:
    print(f"Error: Missing core modules. Please ensure the lib/ directory is properly set up.")
    print(f"Import error: {e}")
    sys.exit(1)


class GrapCLI:
    """Main CLI application class for GRAP"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.parser = None
        self.formatter = None
        
    def setup_logging(self, verbose: int = 0):
        """Configure logging based on verbosity level"""
        levels = [logging.WARNING, logging.INFO, logging.DEBUG]
        level = levels[min(verbose, len(levels) - 1)]
        
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stderr)]
        )
        
    def create_argument_parser(self) -> argparse.ArgumentParser:
        """Create and configure the command line argument parser"""
        parser = argparse.ArgumentParser(
            prog='grap',
            description=__description__,
            epilog="""
Examples:
  %(prog)s galera.log                          # Basic analysis
  %(prog)s --format=json galera.log            # JSON output
  %(prog)s --entities=NODE,STATE_TRANSFER db.log # Filter entities
  %(prog)s --learn --interactive galera.log    # Interactive learning
            """,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        
        # Version
        parser.add_argument(
            '--version', 
            action='version', 
            version=f'%(prog)s {__version__}'
        )
        
        # Verbosity
        parser.add_argument(
            '-v', '--verbose',
            action='count',
            default=0,
            help='Increase verbosity (-v, -vv, -vvv)'
        )
        
        # Input file
        parser.add_argument(
            'logfile',
            nargs='?',
            help='Galera log file to analyze (use - for stdin)'
        )
        
        # Output options
        output_group = parser.add_argument_group('output options')
        output_group.add_argument(
            '--format',
            choices=['text', 'json', 'yaml'],
            default='text',
            help='Output format (default: text)'
        )
        output_group.add_argument(
            '--output', '-o',
            type=Path,
            help='Output file (default: stdout)'
        )
        
        # Entity filtering
        entity_group = parser.add_argument_group('entity options')
        entity_group.add_argument(
            '--entities',
            help='Comma-separated list of entity types to extract (NODE,STATE_TRANSFER,VIEW,etc.)'
        )
        entity_group.add_argument(
            '--confidence-threshold',
            type=float,
            default=0.8,
            help='Minimum confidence threshold for entity extraction (default: 0.8)'
        )
        
        # Pattern options
        pattern_group = parser.add_argument_group('pattern options')
        pattern_group.add_argument(
            '--pattern-version',
            help='Specific MariaDB version patterns to use (e.g., 10.6.8)'
        )
        pattern_group.add_argument(
            '--pattern-dir',
            type=Path,
            default=Path('patterns'),
            help='Directory containing pattern files (default: patterns/)'
        )
        
        # Learning options
        learn_group = parser.add_argument_group('learning options')
        learn_group.add_argument(
            '--learn',
            action='store_true',
            help='Enable interactive learning mode'
        )
        learn_group.add_argument(
            '--interactive',
            action='store_true',
            help='Enable interactive pattern validation'
        )
        learn_group.add_argument(
            '--save-patterns',
            type=Path,
            help='Save learned patterns to specified file'
        )
        
        # Debug options
        debug_group = parser.add_argument_group('debug options')
        debug_group.add_argument(
            '--dry-run',
            action='store_true',
            help='Parse and validate without processing'
        )
        debug_group.add_argument(
            '--show-patterns',
            action='store_true',
            help='Display loaded patterns and exit'
        )
        debug_group.add_argument(
            '--stats',
            action='store_true',
            help='Show parsing statistics'
        )
        
        return parser
        
    def validate_arguments(self, args: argparse.Namespace) -> bool:
        """Validate command line arguments"""
        # Skip file validation for special commands
        if args.show_patterns or args.dry_run:
            return True
            
        # Check input file
        if not args.logfile:
            if sys.stdin.isatty():
                print("Error: No log file provided and no input from stdin", file=sys.stderr)
                return False
            args.logfile = '-'
        elif args.logfile != '-':
            logfile_path = Path(args.logfile)
            if not logfile_path.exists():
                print(f"Error: Log file '{args.logfile}' does not exist", file=sys.stderr)
                return False
            if not logfile_path.is_file():
                print(f"Error: '{args.logfile}' is not a file", file=sys.stderr)
                return False
                
        # Check pattern directory
        if not args.pattern_dir.exists():
            print(f"Error: Pattern directory '{args.pattern_dir}' does not exist", file=sys.stderr)
            return False
            
        # Validate confidence threshold
        if not 0.0 <= args.confidence_threshold <= 1.0:
            print("Error: Confidence threshold must be between 0.0 and 1.0", file=sys.stderr)
            return False
            
        return True
        
    def initialize_components(self, args: argparse.Namespace):
        """Initialize parser and formatter components"""
        try:
            # Initialize entity registry
            entity_registry = EntityRegistry()
            
            # Initialize pattern matcher
            pattern_matcher = PatternMatcher(
                pattern_dir=args.pattern_dir,
                version=args.pattern_version,
                confidence_threshold=args.confidence_threshold
            )
            
            # Initialize parser
            self.parser = LogParser(
                pattern_matcher=pattern_matcher,
                entity_registry=entity_registry,
                learning_mode=args.learn,
                interactive=args.interactive
            )
            
            # Initialize output formatter
            self.formatter = OutputFormatter(
                format_type=args.format,
                show_stats=args.stats
            )
            
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise
            
    def process_log_file(self, args: argparse.Namespace) -> int:
        """Process the log file and generate output"""
        try:
            # Detect dialect before processing
            if args.logfile != '-':
                log_path = Path(args.logfile)
                dialect_info = self.parser.pattern_matcher.detect_dialect_from_file(log_path)
                self.logger.info(f"Detected dialect: {dialect_info.dialect_type.value} (confidence: {dialect_info.confidence:.2f})")
            
            # Parse the log file
            self.logger.info(f"Processing log file: {args.logfile}")
            
            if args.logfile == '-':
                traditional_entities = self.parser.parse_stream(sys.stdin)
            else:
                traditional_entities = self.parser.parse_file(Path(args.logfile))
            
            # Get all entities including temporal entities from session manager
            all_entities = self.parser.get_all_entities()
            
            # Combine traditional and temporal entities, with deduplication
            entities = traditional_entities + all_entities
            
            # Deduplicate temporal entities (they may have same entity_id)
            seen_temporal_ids = set()
            deduplicated_entities = []
            for entity in entities:
                # For temporal entities, use entity_id for deduplication
                if hasattr(entity, 'entity_id') and entity.entity_id:
                    if entity.entity_id in seen_temporal_ids:
                        continue
                    seen_temporal_ids.add(entity.entity_id)
                deduplicated_entities.append(entity)
            
            entities = deduplicated_entities
            
            # Filter entities if requested
            if args.entities:
                entity_types = [t.strip().upper() for t in args.entities.split(',')]
                filtered_entities = []
                for e in entities:
                    # Handle both enum and string entity types
                    entity_type_str = e.entity_type.value if hasattr(e.entity_type, 'value') else str(e.entity_type)
                    if entity_type_str.upper() in entity_types:
                        filtered_entities.append(e)
                entities = filtered_entities
                
            self.logger.info(f"Extracted {len(entities)} entities ({len(traditional_entities)} traditional, {len(all_entities)} temporal)")
            
            
            # Format and output results
            output = self.formatter.format(entities)
            
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(output)
                self.logger.info(f"Results written to: {args.output}")
            else:
                print(output)
                
            # Save learned patterns if requested
            if args.save_patterns and args.learn:
                self.parser.save_learned_patterns(args.save_patterns)
                self.logger.info(f"Learned patterns saved to: {args.save_patterns}")
                
            return 0
            
        except KeyboardInterrupt:
            self.logger.info("Processing interrupted by user")
            return 130
        except Exception as e:
            self.logger.error(f"Processing failed: {e}")
            if args.verbose >= 2:
                import traceback
                traceback.print_exc()
            return 1
            
    def show_patterns(self, args: argparse.Namespace):
        """Display loaded patterns and exit"""
        try:
            pattern_matcher = PatternMatcher(
                pattern_dir=args.pattern_dir,
                version=args.pattern_version
            )
            
            patterns = pattern_matcher.get_all_patterns()
            print(f"Loaded patterns from: {args.pattern_dir}")
            print(f"Pattern version: {args.pattern_version or 'latest'}")
            print(f"Total patterns: {len(patterns)}")
            print()
            
            for entity_type, pattern_list in patterns.items():
                print(f"{entity_type}:")
                for pattern in pattern_list:
                    print(f"  - {pattern.name} (confidence: {pattern.confidence})")
                print()
                
        except Exception as e:
            print(f"Error loading patterns: {e}", file=sys.stderr)
            return 1
            
        return 0
        
    def run(self, args: Optional[List[str]] = None) -> int:
        """Main entry point for the CLI application"""
        try:
            # Parse arguments
            arg_parser = self.create_argument_parser()
            parsed_args = arg_parser.parse_args(args)
            
            # Setup logging
            self.setup_logging(parsed_args.verbose)
            
            # Validate arguments
            if not self.validate_arguments(parsed_args):
                return 1
                
            # Handle special commands
            if parsed_args.show_patterns:
                return self.show_patterns(parsed_args)
                
            if parsed_args.dry_run:
                self.logger.info("Dry run mode - validating configuration only")
                self.initialize_components(parsed_args)
                self.logger.info("Configuration validated successfully")
                return 0
                
            # Initialize components
            self.initialize_components(parsed_args)
            
            # Process the log file
            return self.process_log_file(parsed_args)
            
        except Exception as e:
            print(f"Fatal error: {e}", file=sys.stderr)
            return 1


def main():
    """Entry point for the command line script"""
    cli = GrapCLI()
    sys.exit(cli.run())


if __name__ == '__main__':
    main()