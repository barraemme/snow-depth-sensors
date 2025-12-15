"""Command line interface for snow depth sensors data fetcher."""
from datetime import date
import click
from . import retriever

@click.command()
@click.option(
    '--output', '-o',
    type=click.Path(),
    help='Output file path to save the data (CSV format). If not provided, data is only displayed.'
)
@click.option(
    '--format', '-f',
    type=click.Choice(['csv', 'json', 'geojson'], case_sensitive=False),
    default='csv',
    help='Output format for saved file (default: csv)'
)
@click.option(
    '--timeout',
    type=int,
    default=30,
    help='Request timeout in seconds (default: 30)'
)
@click.option(
    '--date', '-d',
    default=date.today().strftime('%Y-%m-%d'),
    help='Measurement date in YYYY-MM-DD format (default: today)',
)
@click.option(
    '--supabase',
    is_flag=True,
    help='Upload data to Supabase (requires SUPABASE_URL and SUPABASE_SERVICE_KEY env vars)'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Enable verbose output'
)
def main(output, format, timeout, date, supabase, verbose):
    """
    Fetch snow depth sensor data from South Tyrol weather service.
    
    This CLI tool retrieves current snow depth measurements from various sensors
    across South Tyrol and displays or saves the data as a pandas DataFrame.
    """
    
    try:
        retriever.retrieve(
            date=date,
            output=output,
            format=format,
            timeout=timeout,
            verbose=verbose,
            upload_supabase=supabase
        )
        click.echo(f"\n✅ Operation completed successfully!")
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    main()