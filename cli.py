import os
import pandas as pd
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, AuthenticationException, ConnectionError, ApiError
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion, WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.styles import Style

# --- ESQL Keywords for Autocompletion ---
ESQL_KEYWORDS = [
    # Commands
    "FROM", "WHERE", "LIMIT", "GROUP BY", "HAVING", "ORDER BY", "ASC", "DESC",
    "SELECT", "AS", "ROW", "ENRICH", "DROP", "KEEP", "MV_EXPAND", "SORT", 
    "SUBSTRING", "GROK", "DISSECT", "EVAL", "LOOKUP", "METADATA", "STATS",

    # Logical Operators
    "AND", "OR", "NOT",

    # Comparison Operators
    "BETWEEN", "IN", "IS NULL", "IS NOT NULL", "LIKE", "RLIKE",

    # Aggregate Functions
    "AVG", "COUNT", "SUM", "MIN", "MAX", "COUNT_DISTINCT", "FIRST", "LAST",
    "KURTOSIS", "MAD", "PERCENTILE", "PERCENTILE_RANK", "SKEWNESS", 
    "STDDEV_POP", "SUM_OF_SQUARES", "VAR_POP", "VAR_SAMP",

    # Grouping Functions
    "BUCKET", "CATEGORIZE", "HISTOGRAM",

    # Conditional Functions
    "CASE", "COALESCE", "GREATEST", "LEAST",

    # Date/Time Functions
    "DATE_DIFF", "DATE_EXTRACT", "DATE_FORMAT", "DATE_PARSE", "DATE_TRUNC", "NOW",

    # IP Functions
    "CIDR_MATCH", "IP_PREFIX",

    # Math Functions
    "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CBRT", "CEIL", "COS", "COSH", 
    "DEGREES", "E", "EXP", "EXPM1", "FLOOR", "LOG", "LOG10", "PI", "POWER", 
    "RADIANS", "RANDOM", "ROUND", "SIGN", "SIN", "SINH", "SQRT", "TAN", "TANH",

    # String Functions
    "ASCII", "BASE64_DECODE", "BASE64_ENCODE", "CONCAT", "INSERT", "LCASE", 
    "LEFT", "LENGTH", "LOCATE", "LTRIM", "OCTET_LENGTH", "POSITION", 
    "REGEX_EXTRACT", "REGEX_REPLACE", "REPEAT", "REPLACE", "REVERSE", "RIGHT", 
    "RTRIM", "SPACE", "SUBSTR", "TRIM", "UCASE",

    # Type Conversion Functions
    "TO_BOOLEAN", "TO_CARTESIANPOINT", "TO_CARTESIANSHAPE", "TO_DATEPERIOD",
    "TO_DATETIME", "TO_DATE_NANOS", "TO_DEGREES", "TO_DOUBLE", "TO_GEOPOINT",
    "TO_GEOSHAPE", "TO_INTEGER", "TO_IP", "TO_LONG", "TO_RADIANS", "TO_STRING",
    "TO_TIMEDURATION", "TO_UNSIGNED_LONG", "TO_VERSION",

    # Search Functions
    "KQL", "MATCH", "QSTR",

    # Spatial Functions
    "ST_DISTANCE", "ST_INTERSECTS", "ST_DISJOINT", "ST_CONTAINS", "ST_WITHIN"
]


class ESQLCompleter(Completer):
    """
    A custom completer for ESQL that suggests keywords and index names.
    """
    def __init__(self, es_client):
        self.es_client = es_client
        self.keyword_completer = WordCompleter(ESQL_KEYWORDS, ignore_case=True)

    def get_completions(self, document, complete_event):
        """
        Generate completions for the current input.
        """
        text = document.text_before_cursor.upper()
        word_before_cursor = document.get_word_before_cursor(WORD=True)

        # If the user is typing 'FROM', suggest index names
        if "FROM " in text.split()[-2:]:
             try:
                # Synchronously fetch index names
                indices_response = self.es_client.cat.indices(format="json", h="index")
                index_names = [index['index'] for index in indices_response]
                for name in index_names:
                    if name.startswith(word_before_cursor):
                         yield Completion(
                            name,
                            start_position=-len(word_before_cursor),
                            style="fg:ansiblue",
                            selected_style="bg:ansiblue fg:ansiwhite",
                        )
             except (ConnectionError, AuthenticationException) as e:
                # Handle cases where we can't connect to Elasticsearch
                yield Completion(
                    f"Error fetching indices: {e}",
                    start_position=0,
                    style="fg:ansired",
                    selected_style="bg:ansired fg:ansiwhite",
                )

        # Otherwise, yield from the keyword completer
        for completion in self.keyword_completer.get_completions(document, complete_event):
            yield completion


def print_results(response):
    """
    Formats and prints the ESQL query results using Rich and Pandas.
    """
    columns = [col['name'] for col in response['columns']]
    rows = response['values']
    console = Console()

    if not rows:
        console.print("[yellow]Query returned no results.[/yellow]")
        return

    # Create a Pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Create a Rich table
    table = Table(show_header=True, header_style="bold magenta", show_edge=True)

    # Add columns to the table
    for column in df.columns:
        table.add_column(column, style="dim", no_wrap=False, overflow="fold")

    # Add rows to the table
    for _, row in df.iterrows():
        # Convert all items to string for Rich table
        table.add_row(*[str(item) for item in row.values])

    console.print(table)


def main():
    """
    The main function for the ESQL CLI application.
    """
    # Load environment variables from .env file
    load_dotenv()

    console = Console()
    es_client = None
    console.print("[bold cyan]--- Elasticsearch ESQL CLI ---[/bold cyan]")

    try:
        console.print("Connecting to Elasticsearch...")
        
        # --- Connection Details from .env file ---
        es_url = os.environ.get("ELASTICSEARCH_URL")
        es_api_key = os.environ.get("ELASTICSEARCH_API_KEY")

        if es_url and es_api_key:
            console.print(f"Connecting to [bold]{es_url}[/bold] using API key.")
            es_client = Elasticsearch(
                hosts=[es_url],
                api_key=es_api_key,
                request_timeout=10
            )
        else:
            console.print("Environment variables not found. Connecting to [bold]http://localhost:9200[/bold].")
            es_client = Elasticsearch(
                "http://localhost:9200",
                request_timeout=10
            )

        # Check if the connection is successful
        if not es_client.ping():
            raise ConnectionError("Could not connect to Elasticsearch.")
        
        console.print("[green]Successfully connected to Elasticsearch.[/green]")

        # --- Prompt Toolkit Setup ---
        history = FileHistory("esql_history.txt")
        session = PromptSession(
            history=history,
            auto_suggest=AutoSuggestFromHistory(),
            completer=ESQLCompleter(es_client),
            style=Style.from_dict({
                "completion-menu.completion": "bg:#008888 #ffffff",
                "completion-menu.completion.current": "bg:#00aaaa #000000",
                "scrollbar.background": "bg:#88aaaa",
                "scrollbar.button": "bg:#222222",
            }),
        )

        while True:
            try:
                # Use the synchronous prompt
                query = session.prompt("ESQL> ")
                if query.lower().strip() in ["exit", "quit"]:
                    break
                if not query.strip():
                    continue

                # --- Execute Query ---
                resp = es_client.esql.query(query=query)
                print_results(resp.body)

            except ApiError as e:
                console.print(f"[bold red]API Error:[/bold red] {e.message}")
            except Exception as e:
                console.print(f"[bold red]An unexpected error occurred:[/bold red] {e}")

    except (ConnectionError, AuthenticationException) as e:
        console.print(f"[bold red]Error connecting to Elasticsearch:[/bold red] {e}")
    except (KeyboardInterrupt, EOFError):
        # Catch Ctrl+C or Ctrl+D for a clean exit
        pass
    finally:
        console.print("\n[cyan]Exiting ESQL CLI. Goodbye![/cyan]")
        if es_client:
            # Close the synchronous client
            es_client.close()


if __name__ == "__main__":
    main()
