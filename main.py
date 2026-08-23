#!/usr/bin/env python3
"""
Data Ingestion Framework Main Entry Point

Usage:
    python main.py --config config/sample_config.yaml
    python main.py --config config/sample_config.yaml --mode read-only
    python main.py --config config/sample_config.yaml --preview 10
"""

import argparse
import logging
import sys
from src.facade.ingestion_facade import IngestionFacade

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for the ingestion framework"""
    parser = argparse.ArgumentParser(
        description='Data Ingestion Framework - Scalable data ingestion using PySpark'
    )
    
    parser.add_argument(
        '--config',
        required=True,
        help='Path to YAML configuration file'
    )
    
    parser.add_argument(
        '--mode',
        choices=['ingest', 'read-only', 'preview', 'statistics'],
        default='ingest',
        help='Execution mode (default: ingest)'
    )
    
    parser.add_argument(
        '--preview',
        type=int,
        default=10,
        help='Number of rows to preview (default: 10)'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info(f"Initializing Data Ingestion Framework")
        logger.info(f"Configuration: {args.config}")
        logger.info(f"Mode: {args.mode}")
        
        # Initialize facade
        facade = IngestionFacade(args.config)
        
        # Execute based on mode
        if args.mode == 'ingest':
            logger.info("Starting ingestion pipeline...")
            facade.ingest()
            logger.info("Ingestion completed successfully")
            
        elif args.mode == 'read-only':
            logger.info("Starting read-only operation...")
            df = facade.read_only()
            logger.info(f"Read {df.count()} rows with {len(df.columns)} columns")
            
        elif args.mode == 'preview':
            logger.info(f"Previewing first {args.preview} rows...")
            facade.get_source_preview(args.preview)
            
        elif args.mode == 'statistics':
            logger.info("Computing data statistics...")
            stats = facade.get_statistics()
            logger.info(f"Statistics: {stats}")
            for key, value in stats.items():
                print(f"{key}: {value}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
