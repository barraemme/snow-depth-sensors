"""Supabase uploader for snow sensor data."""
import os
import logging
import pandas as pd
import requests
from typing import Optional

logger = logging.getLogger(__name__)


class SupabaseUploader:
    """Upload snow sensor data to Supabase."""
    
    def __init__(self, url: Optional[str] = None, key: Optional[str] = None):
        """
        Initialize Supabase uploader.
        
        Args:
            url: Supabase project URL (or set SUPABASE_URL env var)
            key: Supabase service role key (or set SUPABASE_SERVICE_KEY env var)
        """
        self.url = url or os.environ.get('SUPABASE_URL')
        self.key = key or os.environ.get('SUPABASE_SERVICE_KEY')
        
        if not self.url or not self.key:
            raise ValueError(
                'Supabase URL and service key required. '
                'Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables.'
            )
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'resolution=merge-duplicates',  # Upsert behavior
        }
    
    def upload(self, df: pd.DataFrame, source: str, measurement_date: str) -> dict:
        """
        Upload snow sensor data to Supabase.
        
        Args:
            df: DataFrame with columns: hs, latitude, longitude, name_it/name_de/name_en
            source: Data source identifier ('alto_adige' or 'trentino')
            measurement_date: Date string in YYYY-MM-DD format
            
        Returns:
            dict with upload statistics
        """
        if df.empty:
            logger.warning(f'No data to upload for {source}')
            return {'uploaded': 0, 'source': source}
        
        # Prepare records for upsert
        records = []
        for idx, row in df.iterrows():
            # Skip rows with missing essential data
            if pd.isna(row.get('hs')) or pd.isna(row.get('latitude')) or pd.isna(row.get('longitude')):
                continue
            
            # Get station name (prefer Italian, fallback to others)
            station_name = (
                row.get('name_it') or 
                row.get('name_de') or 
                row.get('name_en') or 
                f'Station_{idx}'
            )
            
            # Generate station code if not present
            station_code = row.get('station_code') or row.get('SCODE') or f'{source}_{idx}'
            
            record = {
                'station_code': str(station_code),
                'station_name': str(station_name),
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude']),
                'hs': float(row['hs']),
                'measurement_date': measurement_date,
                'source': source,
            }
            
            # Optional fields
            if 'hn' in row and pd.notna(row['hn']):
                try:
                    record['hn'] = float(row['hn'])
                except (ValueError, TypeError):
                    pass
            
            if 'elevation' in row and pd.notna(row['elevation']):
                try:
                    record['elevation'] = float(row['elevation'])
                except (ValueError, TypeError):
                    pass
            elif 'ALT' in row and pd.notna(row['ALT']):
                try:
                    record['elevation'] = float(row['ALT'])
                except (ValueError, TypeError):
                    pass
            
            records.append(record)
        
        if not records:
            logger.warning(f'No valid records to upload for {source}')
            return {'uploaded': 0, 'source': source}
        
        # Upload to Supabase
        url = f'{self.url}/rest/v1/snow_sensors'
        
        try:
            response = requests.post(
                url,
                headers=self.headers,
                json=records,
                timeout=60
            )
            response.raise_for_status()
            
            logger.info(f'Successfully uploaded {len(records)} records for {source}')
            return {
                'uploaded': len(records),
                'source': source,
                'date': measurement_date,
            }
            
        except requests.exceptions.HTTPError as e:
            logger.error(f'Supabase upload failed: {e}')
            if response is not None:
                logger.error(f'Response: {response.text}')
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f'Supabase request failed: {e}')
            raise


def upload_to_supabase(df: pd.DataFrame, source: str, measurement_date: str) -> dict:
    """
    Convenience function to upload data to Supabase.
    
    Args:
        df: DataFrame with snow sensor data
        source: Data source ('alto_adige' or 'trentino')
        measurement_date: Date in YYYY-MM-DD format
        
    Returns:
        dict with upload statistics
    """
    uploader = SupabaseUploader()
    return uploader.upload(df, source, measurement_date)
