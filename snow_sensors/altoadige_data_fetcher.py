"""Data fetching module for snow depth sensors."""
import pandas as pd
import requests
from typing import Optional
from io import StringIO
from dateutil import parser as date_parser
import json


class AltoAdigeDataFetcher:
    """Fetches and processes snow depth sensor data from South Tyrol weather service."""
    

    """
    SCODE,TYPE,DESC_D,DESC_I,DESC_L,UNIT,DATE,VALUE
    00390SF,HS,Schneehöhe,Altezza neve al suolo,Autëza dla nëif,cm,2025-09-29T11:20:00CEST,3.0  
    """
    BASE_URL = "http://daten.buergernetz.bz.it/services/meteo/v1/sensors"
    """
    {
			"type" : "Feature",
			"geometry" : {
				"type" : "Point",
				"coordinates" : [ 626295.1443328111, 5164467.604756019 ]
			},
			"properties" : {
				"SCODE" : "08155PG",
				"NAME_D" : "ETSCH BEI EYRS",
				"NAME_I" : "ADIGE A ORIS",
				"NAME_L" : "ETSCH BEI EYRS",
				"NAME_E" : "ETSCH BEI EYRS",
				"ALT" : 873.99,
				"LONG" : 10.64963,
				"LAT" : 46.621876
			}
		},
    """
    STATIONS_URL = "http://dati.retecivica.bz.it/services/meteo/v1/stations"

    def __init__(self):
        self.session = requests.Session()
    
    def fetch_snow_depth_data(self, date, timeout: int = 30) -> pd.DataFrame:
        """
        Fetch snow depth sensor data and return as pandas DataFrame.
        
        Args:
            timeout: Request timeout in seconds
            
        Returns:
            pandas.DataFrame: DataFrame containing sensor data
            
        Raises:
            requests.RequestException: If the HTTP request fails
            pd.errors.ParserError: If CSV parsing fails
        """
        params = {
            'output_format': 'CSV',
            'sensor_code': 'HS'
        }
        
        try:
            response = self.session.get(
                self.BASE_URL, 
                params=params, 
                timeout=timeout
            )
            response.raise_for_status()
            
            # Parse CSV data into DataFrame
            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data, index_col='SCODE')[['DATE', 'VALUE']]
            df.rename(columns={'VALUE': 'hs', 'DATE': 'date'}, inplace=True)
            
            # Convert DATE column to datetime using dateutil for better timezone handling
            if 'date' in df.columns and len(df) > 0:
                def parse_date_safe(date_str):
                    try:
                        parsed_date = date_parser.parse(date_str)
                        # Convert to timezone-naive to avoid comparison issues
                        if parsed_date.tzinfo is not None:
                            parsed_date = parsed_date.replace(tzinfo=None)
                        return parsed_date
                    except (ValueError, TypeError):
                        return pd.NaT

                df['date'] = df['date'].apply(parse_date_safe)

            response = self.session.get(self.STATIONS_URL, timeout=timeout, params={'output_format': 'CSV', 'coord_sys': 'EPSG:4326'})
            response.raise_for_status()

            csv_data = StringIO(response.text)
            stations_df = pd.read_csv(csv_data, index_col='SCODE')[['NAME_D', 'NAME_I', 'NAME_E', 'LONG', 'LAT']]
            stations_df.rename(columns={'NAME_D': 'name_de', 'NAME_I': 'name_it', 'NAME_E': 'name_en', 'LONG': 'longitude', 'LAT': 'latitude'}, inplace=True)

            return df.join(stations_df, how='inner').reset_index(drop=True).dropna()
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON response: {e}")
        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch data from API: {e}")
        except pd.errors.ParserError as e:
            raise pd.errors.ParserError(f"Failed to parse CSV data: {e}")
