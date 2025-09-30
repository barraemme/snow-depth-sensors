#!/usr/bin/env python3
"""
Example script showing how to use the snow depth sensors package directly.
"""
from datetime import date
from snow_sensors.altoadige_data_fetcher import AltoAdigeDataFetcher
import pandas as pd

from snow_sensors.trentino_data_fetcher import TrentinoDataFetcher


def main():
    """Demonstrate usage of the SensorDataFetcher class."""
    
    print("🌨️  Snow Depth Sensors Data Fetcher Example")
    print("=" * 50)
    
    # Create fetcher instance
    fetcher1 = AltoAdigeDataFetcher()
    fetcher2 = TrentinoDataFetcher() 
    
    today = date.today().strftime('%Y-%m-%d')

    try:
        # Fetch the data
        print("Fetching data from South Tyrol weather service...")
        df1 = fetcher1.fetch_snow_depth_data(today)
        df2 = fetcher2.fetch_snow_depth_data(today)

        df = pd.concat([df1, df2], ignore_index=True)

        # print pd.concat([df1, df2, df3, df4], ignore_index=True)
        
        print(f"✅ Successfully fetched {len(df)} records")
        
        # Display basic DataFrame info
        print(f"\n📊 DataFrame Info:")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Memory usage: {df.memory_usage().sum()} bytes")
        
        # Get and display summary statistics
        summary = get_data_summary(df)
        print(f"\n📈 Data Summary:")
        print(f"   Total records: {summary['total_records']}")
        if 'unique_sensors' in summary:
            print(f"   Unique sensors: {summary['unique_sensors']}")
        
        if summary['date_range']:
            print(f"   Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
        
        if summary['value_stats']:
            stats = summary['value_stats']
            print(f"   Snow depth statistics (cm):")
            print(f"     Minimum: {stats['min']:.1f}")
            print(f"     Maximum: {stats['max']:.1f}")
            print(f"     Average: {stats['mean']:.1f}")
            print(f"     Std Dev: {stats['std']:.1f}")
        
        # Show data types
        print(f"\n🔧 Data Types:")
        for col, dtype in df.dtypes.items():
            print(f"   {col}: {dtype}")
        
        # Display first few records
        print(f"\n📋 First 50 Records:")
        print(df.head(n = 50).to_string())
        
        # Show some basic pandas operations
        print(f"\n🔍 Sample Analysis:")

        # Show positive and negative values
        if 'hs' in df.columns:
            positive_values = df[df['hs'] > 0]['hs'].count()
            negative_values = df[df['hs'] < 0]['hs'].count()
            zero_values = df[df['hs'] == 0]['hs'].count()
            
            print(f"   Snow depth distribution:")
            print(f"     Positive values: {positive_values}")
            print(f"     Zero values: {zero_values}")
            print(f"     Negative values: {negative_values}")
        
        # Save sample data
        sample_file = "sample_data.csv"
        df.to_csv(sample_file, index=False)
        print(f"\n💾 Sample data saved to {sample_file}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    print(f"\n✅ Example completed successfully!")
    return 0

def get_data_summary(df: pd.DataFrame) -> dict:
    """
    Get summary statistics for the fetched data.
    
    Args:
        df: DataFrame containing sensor data
        
    Returns:
        dict: Summary statistics
    """
    summary = {
        'total_records': len(df),
        'date_range': None,
        'value_stats': None
    }
    
    # Check for unique sensors using the index column names from both sources
    if len(df) > 0:
        unique_sensors = 0
        if 'SCODE' in df.columns:
            unique_sensors += df['SCODE'].nunique()
        if 'codStaz' in df.columns:
            unique_sensors += df['codStaz'].nunique()
        if unique_sensors > 0:
            summary['unique_sensors'] = unique_sensors
    
    if 'date' in df.columns and len(df) > 0 and not df['date'].isna().all():
        summary['date_range'] = {
            'start': df['date'].min(),
            'end': df['date'].max()
        }
    
    if 'hs' in df.columns and len(df) > 0:
        # Convert VALUE to numeric, handling any non-numeric values
        numeric_values = pd.to_numeric(df['hs'], errors='coerce')
        valid_values = numeric_values.dropna()
        
        if len(valid_values) > 0:
            summary['value_stats'] = {
                'min': valid_values.min(),
                'max': valid_values.max(),
                'mean': valid_values.mean(),
                'std': valid_values.std()
            }
    
    return summary


if __name__ == "__main__":
    exit(main())