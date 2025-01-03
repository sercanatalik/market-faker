from typing import Optional
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client

def create_risk_materialized_view(client: Client) -> None:
    """
    Creates a materialized view that joins risk, trade, counterparty, and instrument data.
    
    Args:
        client: ClickHouse client instance
        
    Raises:
        Exception: If there's an error executing the query
    """
    drop_view_query = "DROP TABLE IF EXISTS risk_f_mv"
    
    create_view_query = """
        CREATE MATERIALIZED VIEW risk_f_mv
        ENGINE = ReplacingMergeTree(calculatedAt)
        PRIMARY KEY (r.asOfDate, r.id)
        ORDER BY (r.asOfDate, r.id)
        AS
        SELECT
            r.asOfDate,
            r.id,
            -- Risk fields
            r.jobId AS risk_jobId,
            r.snapId AS risk_snapId,
            r.tradeId AS risk_tradeId,
            r.counterParty,
            r.collatId,
            r.collatDesc,
            r.collatConcentration,
            r.collatName,
            r.collatTicker,
            r.collatIssuer,
            r.outstandingAmt,
            r.dtm,
            r.age,
            r.tenor,
            r.fxSpot,
            r.fxSpotFunding,
            r.fxSpotEOD,
            r.fundingAmount,
            r.collateralAmount,
            r.cashOut,
            r.accrualDaily,
            r.accrualProjected,
            r.accrualRealised,
            r.pxEOD,
            r.pxLast,
            r.realizedMarginCall,
            r.expectedMarginCall,
            r.financingExposure,
            r.calculatedAt,
            
            -- Trade fields (excluding duplicates)
            t.jobId AS trade_jobId,
            t.snapId AS trade_snapId,
            t.version,
            t.tradeId,
            t.status AS trade_status,
            t.pts,
            t.hmsBook,
            t.productType,
            t.productSubType,
            t.tradeDt,
            t.startDt,
            t.maturityDt,
            t.maturityIsOpen,
            t.executionDt,
            t.treatsCode,
            t.traderName,
            t.projectName,
            t.qmlError,
            t.model,
            t.side,
            t.haircut,
            t.collatCurrency,
            t.settlementCurrency,
            t.collatType,
            t.fundingLegType,
            t.fundingLegNotional,
            t.fundingLegCurrency,
            t.fundingLegMargin,
            t.fundingLegFixingLabel,
            t.iaAmount,
            t.iaCcy,
            t.pxInception,
            t.pxInceptionClean,
            t.pxFactorInception,
            t.sideFactor,
            t.fxPair,
            t.fxPairFunding,
            
            -- Counterparty fields
            cp.name AS counterparty_name,
            cp.shortName AS counterparty_shortName,
            cp.type AS counterparty_type,
            cp.region AS counterparty_region,
            cp.country AS counterparty_country,
            cp.sector AS counterparty_sector,
            cp.industry AS counterparty_industry,
            cp.rating AS counterparty_rating,
            cp.ratingAgency AS counterparty_ratingAgency,
            cp.lei AS counterparty_lei,
            cp.status AS counterparty_status,
            cp.updatedAt AS counterparty_updatedAt,
            
            -- Instrument fields
            i.type AS instrument_type,
            i.name AS instrument_name,
            i.description AS instrument_description,
            i.status AS instrument_status,
            i.updatedAt AS instrument_updatedAt,

            -- HMS Book fields
            h.desk AS hmsbook_desk,
            h.updatedAt AS hmsbook_updatedAt
            
        FROM risk_f AS r
        LEFT JOIN trades_f AS t ON r.id = t.id
        LEFT JOIN counterparty_f AS cp ON r.counterParty = cp.name
        LEFT JOIN instrument_f AS i ON r.collatId = i.id
        LEFT JOIN hmsbook_f AS h ON t.hmsBook = h.name
    """

    try:
        client.command(drop_view_query)
        client.command(create_view_query)
        print("Materialized view 'risk_f_mv' created successfully")
    except Exception as e:
        print(f"Error creating materialized view: {str(e)}")
        raise

def get_clickhouse_client(
    host: str = "localhost",
    port: int = 8123,
    username: str = "default",
    password: str = ""
) -> Client:
    """
    Creates and returns a ClickHouse client instance.
    
    Returns:
        Client: Configured ClickHouse client
    """
    return get_client(
        host=host,
        port=port,
        username=username,
        password=password
    )

if __name__ == "__main__":
    client = get_clickhouse_client()
    create_risk_materialized_view(client)
