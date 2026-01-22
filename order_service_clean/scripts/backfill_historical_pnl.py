#!/usr/bin/env python3
"""
Historical P&L Backfill Script

Backfills strategy_pnl_metrics table with historical P&L data from past trades.

Usage:
    python backfill_historical_pnl.py [--strategy-id ID] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

Examples:
    # Backfill all strategies for last 30 days
    python backfill_historical_pnl.py

    # Backfill specific strategy
    python backfill_historical_pnl.py --strategy-id 12

    # Backfill date range
    python backfill_historical_pnl.py --start-date 2025-11-01 --end-date 2025-11-24
"""
import asyncio
import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.config.settings import settings
from app.services.pnl_calculator import PnLCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HistoricalPnLBackfill:
    """Backfill historical P&L metrics for strategies"""

    def __init__(self, db_url: str):
        """Initialize backfill service"""
        self.engine = create_async_engine(db_url, echo=False)
        self.async_session = sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def get_strategies_with_trades(self, strategy_id: int = None):
        """
        Get strategies that have trades (need P&L calculation).

        Args:
            strategy_id: Optional specific strategy ID

        Returns:
            List of (strategy_id, first_trade_date, last_trade_date)
        """
        async with self.async_session() as session:
            query = """
                SELECT
                    strategy_id,
                    MIN(DATE(trade_time)) as first_trade_date,
                    MAX(DATE(trade_time)) as last_trade_date,
                    COUNT(*) as trade_count
                FROM order_service.trades
            """

            if strategy_id:
                query += f" WHERE strategy_id = :strategy_id"

            query += " GROUP BY strategy_id ORDER BY strategy_id"

            result = await session.execute(
                text(query),
                {"strategy_id": strategy_id} if strategy_id else {}
            )

            strategies = []
            for row in result.fetchall():
                strategies.append({
                    "strategy_id": row.strategy_id,
                    "first_trade_date": row.first_trade_date,
                    "last_trade_date": row.last_trade_date,
                    "trade_count": row.trade_count
                })

            return strategies

    async def get_existing_pnl_dates(self, strategy_id: int):
        """
        Get dates that already have P&L metrics.

        Args:
            strategy_id: Strategy ID

        Returns:
            Set of dates with existing metrics
        """
        async with self.async_session() as session:
            result = await session.execute(
                text("""
                    SELECT metric_date
                    FROM public.strategy_pnl_metrics
                    WHERE strategy_id = :strategy_id
                """),
                {"strategy_id": strategy_id}
            )

            return {row.metric_date for row in result.fetchall()}

    async def backfill_strategy_date_range(
        self,
        strategy_id: int,
        start_date: date,
        end_date: date,
        force: bool = False
    ) -> dict:
        """
        Backfill P&L metrics for a strategy over a date range.

        Args:
            strategy_id: Strategy ID
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            force: If True, overwrite existing metrics

        Returns:
            Dict with backfill statistics
        """
        logger.info(
            f"Backfilling strategy {strategy_id}: "
            f"{start_date} to {end_date} (force={force})"
        )

        # Get existing dates (skip if not forcing)
        existing_dates = set()
        if not force:
            existing_dates = await self.get_existing_pnl_dates(strategy_id)

        # Process each date
        dates_processed = 0
        dates_skipped = 0
        dates_failed = 0

        current_date = start_date
        while current_date <= end_date:
            try:
                # Skip weekends (markets closed)
                if current_date.weekday() >= 5:  # Saturday=5, Sunday=6
                    current_date += timedelta(days=1)
                    continue

                # Skip if already exists (unless forcing)
                if not force and current_date in existing_dates:
                    logger.debug(f"Skipping {current_date} (already exists)")
                    dates_skipped += 1
                    current_date += timedelta(days=1)
                    continue

                # Calculate P&L for this date
                async with self.async_session() as session:
                    pnl_calculator = PnLCalculator(session)
                    success = await pnl_calculator.update_strategy_pnl_metrics(
                        strategy_id=strategy_id,
                        trading_day=current_date
                    )

                    if success:
                        dates_processed += 1
                        logger.info(f"✅ Backfilled {current_date} for strategy {strategy_id}")
                    else:
                        dates_failed += 1
                        logger.warning(f"⚠️  Failed to backfill {current_date}")

            except Exception as e:
                dates_failed += 1
                logger.error(f"Error backfilling {current_date}: {e}", exc_info=True)

            current_date += timedelta(days=1)

        return {
            "strategy_id": strategy_id,
            "start_date": start_date,
            "end_date": end_date,
            "dates_processed": dates_processed,
            "dates_skipped": dates_skipped,
            "dates_failed": dates_failed,
            "total_dates": (end_date - start_date).days + 1
        }

    async def backfill_all_strategies(
        self,
        start_date: date = None,
        end_date: date = None,
        force: bool = False
    ) -> list:
        """
        Backfill all strategies with trades.

        Args:
            start_date: Start date (defaults to earliest trade)
            end_date: End date (defaults to today)
            force: If True, overwrite existing metrics

        Returns:
            List of backfill results per strategy
        """
        if end_date is None:
            end_date = date.today()

        # Get strategies with trades
        strategies = await self.get_strategies_with_trades()

        if not strategies:
            logger.warning("No strategies with trades found")
            return []

        logger.info(f"Found {len(strategies)} strategies with trades")

        # Backfill each strategy
        results = []
        for strategy_info in strategies:
            strategy_id = strategy_info["strategy_id"]

            # Use strategy's first trade date if no start_date provided
            strategy_start = start_date or strategy_info["first_trade_date"]
            strategy_end = min(end_date, strategy_info["last_trade_date"])

            logger.info(
                f"\n{'='*60}\n"
                f"Strategy {strategy_id}: {strategy_info['trade_count']} trades "
                f"({strategy_start} to {strategy_end})\n"
                f"{'='*60}"
            )

            result = await self.backfill_strategy_date_range(
                strategy_id=strategy_id,
                start_date=strategy_start,
                end_date=strategy_end,
                force=force
            )

            results.append(result)

        return results

    async def backfill_single_strategy(
        self,
        strategy_id: int,
        start_date: date = None,
        end_date: date = None,
        force: bool = False
    ) -> dict:
        """
        Backfill a single strategy.

        Args:
            strategy_id: Strategy ID
            start_date: Start date (defaults to earliest trade)
            end_date: End date (defaults to today)
            force: If True, overwrite existing metrics

        Returns:
            Backfill result dict
        """
        if end_date is None:
            end_date = date.today()

        # Get strategy trade info
        strategies = await self.get_strategies_with_trades(strategy_id)

        if not strategies:
            logger.error(f"Strategy {strategy_id} has no trades")
            return {
                "strategy_id": strategy_id,
                "error": "No trades found"
            }

        strategy_info = strategies[0]
        strategy_start = start_date or strategy_info["first_trade_date"]
        strategy_end = min(end_date, strategy_info["last_trade_date"])

        logger.info(
            f"Backfilling strategy {strategy_id}: {strategy_info['trade_count']} trades "
            f"({strategy_start} to {strategy_end})"
        )

        result = await self.backfill_strategy_date_range(
            strategy_id=strategy_id,
            start_date=strategy_start,
            end_date=strategy_end,
            force=force
        )

        return result

    async def close(self):
        """Close database engine"""
        await self.engine.dispose()


async def main():
    """Main backfill function"""
    parser = argparse.ArgumentParser(
        description="Backfill historical P&L metrics for strategies"
    )
    parser.add_argument(
        "--strategy-id",
        type=int,
        help="Backfill specific strategy ID (default: all strategies)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date YYYY-MM-DD (default: earliest trade date)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date YYYY-MM-DD (default: today)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing metrics (default: skip existing)"
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=settings.DATABASE_URL,
        help="Database URL (default: from settings)"
    )

    args = parser.parse_args()

    # Parse dates
    start_date = None
    end_date = None

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid start date format: {args.start_date}")
            return 1

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid end date format: {args.end_date}")
            return 1

    # Initialize backfill service
    backfill = HistoricalPnLBackfill(args.db_url)

    try:
        # Backfill
        if args.strategy_id:
            # Single strategy
            result = await backfill.backfill_single_strategy(
                strategy_id=args.strategy_id,
                start_date=start_date,
                end_date=end_date,
                force=args.force
            )
            results = [result]
        else:
            # All strategies
            results = await backfill.backfill_all_strategies(
                start_date=start_date,
                end_date=end_date,
                force=args.force
            )

        # Print summary
        print("\n" + "="*60)
        print("BACKFILL SUMMARY")
        print("="*60)

        total_processed = sum(r.get("dates_processed", 0) for r in results)
        total_skipped = sum(r.get("dates_skipped", 0) for r in results)
        total_failed = sum(r.get("dates_failed", 0) for r in results)

        for result in results:
            if "error" in result:
                print(f"\n❌ Strategy {result['strategy_id']}: {result['error']}")
            else:
                print(
                    f"\n✅ Strategy {result['strategy_id']}: "
                    f"{result['dates_processed']} processed, "
                    f"{result['dates_skipped']} skipped, "
                    f"{result['dates_failed']} failed"
                )

        print(f"\n{'='*60}")
        print(f"Total: {total_processed} processed, {total_skipped} skipped, {total_failed} failed")
        print(f"{'='*60}\n")

        return 0 if total_failed == 0 else 1

    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        return 1

    finally:
        await backfill.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
