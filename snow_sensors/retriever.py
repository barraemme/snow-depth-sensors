from .altoadige_data_fetcher import AltoAdigeDataFetcher
from .trentino_data_fetcher import TrentinoDataFetcher
import pandas as pd
import geopandas as gpd
import logging
import os
from pathlib import Path


def retrieve(date, output, format, timeout, verbose, upload_supabase=True):
    if verbose:
        logging.info("Initializing data fetcher...")
    
    fetcher1 = AltoAdigeDataFetcher()
    fetcher2 = TrentinoDataFetcher()
    
    if verbose:
        logging.info(f"Fetching data from API (timeout: {timeout}s)...")
    
    # Fetch the data
    df1 = fetcher1.fetch_snow_depth_data(date=date, timeout=timeout)
    df2 = fetcher2.fetch_snow_depth_data(date=date, timeout=timeout)
    
    # Upload to Supabase if enabled
    if upload_supabase:
        _upload_to_supabase(df1, df2, date, verbose)

    df = pd.concat([df1, df2], ignore_index=True)
    
    if verbose:
        logging.info(f"Successfully fetched {len(df)} records")
    
    # Display basic info about the DataFrame
    logging.info(f"\n📊 Data Summary:")
    logging.info(f"   Records: {len(df)}")
    logging.info(f"   Columns: {list(df.columns)}")
    # Display first few rows
    logging.info(f"\n📋 First 5 records:")
    logging.info(df.head().to_string())
    
    # Save data if output path is provided
    if output:
        output_path = Path(output)
        if verbose:
            logging.info(f"\nSaving data to {output_path} in {format} format...")
        try:
            if format == 'csv':
                df.to_csv(output_path, index=False)
            elif format == 'json':
                df.to_json(output_path, orient='records', date_format='iso')
            elif format == 'geojson':
                if 'latitude' in df.columns and 'longitude' in df.columns:
                    gdf = gpd.GeoDataFrame(
                        df,
                        geometry=gpd.points_from_xy(df.longitude, df.latitude),
                        crs='EPSG:4326',
                    )
                    gdf.to_file(output_path, driver='GeoJSON')
                else:
                    raise ValueError("DataFrame must contain 'latitude' and 'longitude' columns for GeoJSON format.")
            logging.info(f"✅ Data saved successfully to {output_path}")
        except Exception as e:
            logging.error(f"❌ Error saving file: {e}")
            raise
    
    logging.info(f"\n✅ Operation completed successfully!")
    return df


def _upload_to_supabase(df1: pd.DataFrame, df2: pd.DataFrame, date: str, verbose: bool):
    """Upload data to Supabase if configured."""
    if not os.environ.get('SUPABASE_URL') or not os.environ.get('SUPABASE_SERVICE_KEY'):
        if verbose:
            logging.warning("Supabase not configured (SUPABASE_URL and SUPABASE_SERVICE_KEY required)")
        return
    
    try:
        from .supabase_uploader import upload_to_supabase
        
        # Upload Alto Adige data
        if not df1.empty:
            result1 = upload_to_supabase(df1, 'alto_adige', date)
            logging.info(f"📤 Supabase: uploaded {result1['uploaded']} Alto Adige records")
        
        # Upload Trentino data
        if not df2.empty:
            result2 = upload_to_supabase(df2, 'trentino', date)
            logging.info(f"📤 Supabase: uploaded {result2['uploaded']} Trentino records")
            
    except Exception as e:
        logging.error(f"❌ Supabase upload failed: {e}")

