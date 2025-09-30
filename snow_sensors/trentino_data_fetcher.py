"""Data fetching module for snow depth sensors."""
import pandas as pd
import requests
from io import StringIO
from dateutil import parser as date_parser
import json

class TrentinoDataFetcher:
    """Fetches and processes snow depth sensor data from South Tyrol weather service."""
    """
    codice_campo;nome;luogo;Altitudine;Esposizione;LatUTM;LongUTM;DataRilievo;Ora;Anno;Mese;Giorno;Stagione;WW;N;V;VQ1;VQ2;Ta;TaG;Tmin;TminG;Tmax;TmaxG;HS;HN;HSnum;HNnum;pioggia;CheckCum;CheckVen;HnCum;HNStim;rho;TH010;TH01G;TH030;TH03G;PR;CS;S;B;
    21MB;BISSINA;MALGA BISSINA;1780;E ;617129,7038;5101039,8975;30/09/2025 00:00:00;800;2025;9;30;2025/26;44;1;4;0;0;03;3  ;03;3  ;12;12 ;000;000;0  ;0  ;;False;False;;;///;//;// ;//;// ;///;//;/;/;
    """
    BASE_URL = "https://dati.meteotrentino.it/service.asmx/rilieviNeveDiUnGiornoCsv"
    STATIONS_URL = "https://dati.meteotrentino.it/service.asmx/listaCampiNeveXml"


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
        
        try:
            response = self.session.get(
                self.BASE_URL,  
                timeout=timeout,
                params={
                    'anno': date.split('-')[0],
                    'mese': date.split('-')[1],
                    'giorno': date.split('-')[2]
                }
            )
            response.raise_for_status()
            
            # Parse XML data into DataFrame
            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data, delimiter=';', index_col='codice_campo')
            print(df.head())
            df.rename(columns={'HS': 'hs', 'HN': 'hn', 'DataRilievo': 'date'}, inplace=True)
            
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

            response = self.session.get(self.STATIONS_URL, timeout=timeout)
            response.raise_for_status()

            xml_data = StringIO(response.text)
            # Parse only camponeve elements, handling XML namespace
            stations_df = pd.read_xml(
                xml_data,
                xpath='.//ns:camponeve',
                namespaces={'ns': 'http://www.meteotrentino.it/'},
                parser='etree'
            )
            
            stations_df = stations_df.set_index('codice')
            print(stations_df.head())
            # Use 'nome' for name_it, name_en, name_de
            stations_df.rename(columns={'nome': 'name_it', 'lat': 'latitude', 'lon': 'longitude'}, inplace=True)
            stations_df['name_en'] = stations_df['name_it']
            stations_df['name_de'] = stations_df['name_it']

            reset_df = df.join(stations_df, how='inner').reset_index(drop=True)
            return reset_df[['date', 'hs', 'hn', 'name_it', 'name_en', 'name_de', 'longitude', 'latitude']]
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON response: {e}")
        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch data from API: {e}")
        except pd.errors.ParserError as e:
            raise pd.errors.ParserError(f"Failed to parse CSV data: {e}")
