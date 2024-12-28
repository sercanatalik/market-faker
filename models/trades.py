from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Type, Any, get_args, get_origin, List, Union
from pydantic import BaseModel, Field
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from faker import Faker
import random
import polars as pl

# Previous type mapping and helper function remain the same
PYTHON_TO_CLICKHOUSE_TYPE_MAP = {
    str: "String",
    int: "Int64",
    float: "Float64",
    bool: "UInt8",
    date: "Date",
    datetime: "DateTime",
    Decimal: "Decimal(38, 18)",
}

PYTHON_TO_POLARS_TYPE_MAP = {
    str: pl.Utf8,
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    date: pl.Date,
    datetime: pl.Datetime,
    Decimal: pl.Decimal(38, 18),
}

def get_clickhouse_type(python_type: Type) -> str:
    origin = get_origin(python_type)
    if origin is Optional:
        base_type = get_args(python_type)[0]
        if base_type is Decimal:
            return "Nullable(Decimal(38, 18))"
        return f"Nullable({PYTHON_TO_CLICKHOUSE_TYPE_MAP.get(base_type, 'String')})"
    return PYTHON_TO_CLICKHOUSE_TYPE_MAP.get(python_type, "String")

def get_polars_type(python_type: Type) -> pl.DataType:
    origin = get_origin(python_type)
    if origin is Optional:
        base_type = get_args(python_type)[0]
        if base_type is Decimal:
            return pl.Decimal128(38, 18)
        return PYTHON_TO_POLARS_TYPE_MAP.get(base_type, pl.Utf8)
    return PYTHON_TO_POLARS_TYPE_MAP.get(python_type, pl.Utf8)


class Risk(BaseModel):
    jobId: Optional[str]
    asOfDate: datetime
    snapId: Optional[str]
    id: int
    tradeId: Optional[str]
    counterParty: Optional[str]
    collatId: Optional[str]
    collatDesc: Optional[str]
    collatConcentration: Optional[float]
    collatName: Optional[str]
    collatTicker: Optional[str]
    collatIssuer: Optional[str]
    outstandingAmt: Optional[Decimal] = Field(max_digits=38, decimal_places=18)
    dtm: Optional[str]
    age: Optional[str]
    tenor: Optional[str]
    fxSpot: Optional[float]
    fxSpotFunding: Optional[float]
    fxSpotEOD: Optional[float]
    fundingAmount: Optional[Decimal] = Field(max_digits=38, decimal_places=18)
    collateralAmount: Optional[Decimal] = Field(max_digits=38, decimal_places=18)
    cashOut: Optional[Decimal] = Field(max_digits=38, decimal_places=18)
    accrualDaily: Optional[Decimal] = Field(max_digits=38, decimal_places=18)
    accrualProjected: Optional[Decimal] = Field(max_digits=38, decimal_places=18)
    accrualRealised: Optional[Decimal] = Field(max_digits=38, decimal_places=18)
    pxEOD: Optional[Decimal] = Field(max_digits=38, decimal_places=18)
    pxLast: Optional[float]
    realizedMarginCall: Optional[float]
    expectedMarginCall: Optional[float]
    financingExposure: Optional[float]
    calculatedAt: datetime




class Trade(BaseModel):
    # Previous model fields remain the same
    asOfDate: date
    jobId: Optional[str] = None
    snapId: Optional[str] = None
    id: int
    version: int
    tradeId: Optional[str] = None
    status: Optional[str] = None
    pts: Optional[str] = None
    hmsBook: Optional[str] = None
    productType: Optional[str] = None
    productSubType: Optional[str] = None
    tradeDt: Optional[date] = None
    startDt: Optional[date] = None
    maturityDt: Optional[date] = None
    maturityIsOpen: Optional[str] = None
    executionDt: Optional[datetime] = None
    counterParty: Optional[str] = None
    treatsCode: Optional[str] = None
    traderName: Optional[str] = None
    projectName: Optional[str] = None
    qmlError: Optional[str] = None
    model: Optional[str] = None
    side: Optional[str] = None
    haircut: Optional[float] = None
    collatCurrency: Optional[str] = None
    settlementCurrency: Optional[str] = None
    collatId: Optional[str] = None
    collatDesc: Optional[str] = None
    collatNotional: Optional[Decimal] = Field(None, max_digits=38, decimal_places=18)
    collatType: Optional[str] = None
    fundingLegType: Optional[str] = None
    fundingLegNotional: Optional[Decimal] = Field(None, max_digits=38, decimal_places=18)
    fundingLegCurrency: Optional[str] = None
    fundingLegMargin: Optional[Decimal] = Field(None, max_digits=38, decimal_places=18)
    fundingLegFixingLabel: Optional[str] = None
    iaAmount: Optional[Decimal] = Field(None, max_digits=38, decimal_places=18)
    iaCcy: Optional[str] = None
    pxInception: Optional[float] = None
    pxInceptionClean: Optional[float] = None
    pxFactorInception: Optional[float] = None
    sideFactor: Optional[float] = None
    fxPair: Optional[str] = None
    fxPairFunding: Optional[str] = None


    @classmethod
    def create_clickhouse_table(cls, client: Client, table_name: str='trades_f', database: str = "default", drop_existing: bool = True,
                              order_by: tuple = ("asOfDate", "id")) -> None:
        """
        Dynamically creates a ClickHouse table based on the Pydantic model fields.
        
        Args:
            client: ClickHouse client instance
            table_name: Name of the table to create
            database: Database name (defaults to 'default')
            order_by: Tuple of field names to use for ordering (defaults to ("asOfDate", "id"))
        """
        # First drop the existing table if it exists
        if drop_existing:
            drop_table_query = f"DROP TABLE IF EXISTS {database}.{table_name}"
            client.command(drop_table_query)
        
        # Get model fields and their types
        field_definitions = []
        for field_name, field in cls.model_fields.items():
            # Get the field type
            field_type = field.annotation
            clickhouse_type = get_clickhouse_type(field_type)
            field_definitions.append(f"{field_name} {clickhouse_type}")
        
        # Join field definitions with commas
        fields_sql = ",\n    ".join(field_definitions)
        
        # Create the ORDER BY clause
        order_by_clause = ", ".join(order_by)
        
        # Construct the complete CREATE TABLE query
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name} (
            {fields_sql}
        ) ENGINE = ReplacingMergeTree(version)
        ORDER BY ({order_by_clause});
        """
        
        client.command(create_table_query)

        

    @classmethod
    def generate_random_trades_df(cls, num_records: int = 10) -> pl.DataFrame:
        """
        Generates random trade records and returns them as a Polars DataFrame.
        
        Args:
            num_records: Number of records to generate (default: 10)
            
        Returns:
            pl.DataFrame: Polars DataFrame containing the random trades
        """
        fake = Faker()
        
        # Define realistic values for specific fields
        status_options = ['NEW', 'PENDING', 'COMPLETED', 'CANCELLED']
        product_types = ['COLLATERAL', 'SWAP', 'FUTURE', 'OPTION']
        product_subtypes = ['CLR', 'STD', 'FWD']
        currencies = ['USD', 'EUR', 'GBP', 'JPY']
        books = ['INTSTRREPO', 'EXTSTRREPO', 'EQTYREPO']
        collat_types = ['EURABS', 'USABS', 'GOVTBOND']
        funding_leg_types = ['fixedRate', 'floatingRate']
        fx_pairs = ['USDEUR', 'USDJPY', 'EURGBP', 'EURUSD']
        
        base_date = date.today()
        
        # Initialize lists for each column
        data = {
            'asOfDate': [],
            'jobId': [],
            'snapId': [],
            'id': [],
            'version': [],
            'tradeId': [],
            'status': [],
            'pts': [],
            'hmsBook': [],
            'productType': [],
            'productSubType': [],
            'tradeDt': [],
            'startDt': [],
            'maturityDt': [],
            'maturityIsOpen': [],
            'executionDt': [],
            'counterParty': [],
            'treatsCode': [],
            'traderName': [],
            'projectName': [],
            'qmlError': [],
            'model': [],
            'side': [],
            'haircut': [],
            'collatCurrency': [],
            'settlementCurrency': [],
            'collatId': [],
            'collatDesc': [],
            'collatNotional': [],
            'collatType': [],
            'fundingLegType': [],
            'fundingLegNotional': [],
            'fundingLegCurrency': [],
            'fundingLegMargin': [],
            'fundingLegFixingLabel': [],
            'iaAmount': [],
            'iaCcy': [],
            'pxInception': [],
            'pxInceptionClean': [],
            'pxFactorInception': [],
            'sideFactor': [],
            'fxPair': [],
            'fxPairFunding': []
        }
        
        for _ in range(num_records):
            trade_date = base_date - timedelta(days=random.randint(0, 365))
            maturity_date = trade_date + timedelta(days=random.randint(30, 1825))
            execution_time = datetime.combine(trade_date, 
                                           datetime.min.time()) + timedelta(hours=random.randint(8, 16),
                                                                          minutes=random.randint(0, 59))
            
            data['asOfDate'].append(base_date)
            data['jobId'].append(fake.uuid4())
            data['snapId'].append(f"REPO:{base_date.strftime('%Y%m%d')}")
            data['id'].append(fake.unique.random_number(digits=15))
            data['version'].append(0)
            data['tradeId'].append(str(fake.random_number(digits=8)))
            data['status'].append(random.choice(status_options))
            data['pts'].append(random.choice(['MARTINI', 'MANHATTAN', 'MOJITO']))
            data['hmsBook'].append(random.choice(books))
            data['productType'].append(random.choice(product_types))
            data['productSubType'].append(random.choice(product_subtypes))
            data['tradeDt'].append(trade_date.strftime('%Y-%m-%d'))
            data['startDt'].append(trade_date.strftime('%Y-%m-%d'))
            data['maturityDt'].append(maturity_date.strftime('%Y-%m-%d'))
            data['maturityIsOpen'].append(str(random.choice([True, False])).lower())
            data['executionDt'].append(execution_time.strftime('%Y-%m-%d %H:%M:%S'))
            data['counterParty'].append(f"CP{fake.random_number(digits=6)}")
            data['treatsCode'].append(f"TC{fake.random_number(digits=6)}")
            data['traderName'].append(fake.last_name().upper())
            data['projectName'].append(None if random.random() < 0.7 else f"PROJ_{fake.random_number(digits=4)}")
            data['qmlError'].append(None)
            data['model'].append(None)
            data['side'].append(None)
            data['haircut'].append(round(random.uniform(0, 0.1), 4))
            data['collatCurrency'].append(random.choice(currencies))
            data['settlementCurrency'].append(random.choice(currencies))
            data['collatId'].append(f"XS{fake.random_number(digits=10)}")
            data['collatDesc'].append(f"XS{fake.random_number(digits=10)}")
            data['collatNotional'].append(Decimal(str(round(random.uniform(1000000, 50000000), 2))))
            data['collatType'].append(random.choice(collat_types))
            data['fundingLegType'].append(random.choice(funding_leg_types))
            data['fundingLegNotional'].append(Decimal(str(round(random.uniform(1000000, 50000000), 2))))
            data['fundingLegCurrency'].append(random.choice(currencies))
            data['fundingLegMargin'].append(Decimal(str(round(random.uniform(0, 0.1), 4))))
            data['fundingLegFixingLabel'].append(None)
            data['iaAmount'].append(None)
            data['iaCcy'].append(None)
            data['pxInception'].append(round(random.uniform(0.8, 1.2), 6))
            data['pxInceptionClean'].append(round(random.uniform(0.8, 1.2), 6))
            data['pxFactorInception'].append(None)
            data['sideFactor'].append(1.0)
            data['fxPair'].append(random.choice(fx_pairs))
            data['fxPairFunding'].append(random.choice(fx_pairs))
        
        schema = {field_name: get_polars_type(field.annotation) 
                 for field_name, field in cls.model_fields.items()}
        return pl.DataFrame(data, schema=schema,strict=False)
    
    @classmethod
    def save_to_clickhouse(cls, df: pl.DataFrame, client: Client, table_name: str='trades_f', database: str = "default") -> None:
        df = df.to_arrow()
        client.insert_arrow(f"{database}.{table_name}", df)

# Example usage:
if __name__ == "__main__":
    client = get_client(host='localhost', port=8123, username='default', password='')
    Trade.create_clickhouse_table(client, drop_existing=True)
    df = Trade.generate_random_trades_df(10)
    Trade.save_to_clickhouse(df, client, table_name='trades_f', database='default')

    
    print(df.glimpse())