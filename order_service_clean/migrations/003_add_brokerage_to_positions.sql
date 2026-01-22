-- Add brokerage and charges columns to positions table
-- Enables accurate net P&L calculation including all trading costs

ALTER TABLE positions
ADD COLUMN IF NOT EXISTS total_charges NUMERIC(18, 2) NOT NULL DEFAULT 0;

ALTER TABLE positions
ADD COLUMN IF NOT EXISTS brokerage NUMERIC(18, 2) NOT NULL DEFAULT 0;

ALTER TABLE positions
ADD COLUMN IF NOT EXISTS stt NUMERIC(18, 2) NOT NULL DEFAULT 0;

ALTER TABLE positions
ADD COLUMN IF NOT EXISTS exchange_charges NUMERIC(18, 2) NOT NULL DEFAULT 0;

ALTER TABLE positions
ADD COLUMN IF NOT EXISTS gst NUMERIC(18, 2) NOT NULL DEFAULT 0;

ALTER TABLE positions
ADD COLUMN IF NOT EXISTS net_pnl NUMERIC(18, 2) NOT NULL DEFAULT 0;

-- Update column comments for clarity
COMMENT ON COLUMN positions.realized_pnl IS 'Realized P&L (gross, before charges)';
COMMENT ON COLUMN positions.unrealized_pnl IS 'Unrealized P&L (gross, before charges)';
COMMENT ON COLUMN positions.total_pnl IS 'Total P&L (gross, realized + unrealized)';

COMMENT ON COLUMN positions.total_charges IS 'Total brokerage + charges';
COMMENT ON COLUMN positions.brokerage IS 'Brokerage fees';
COMMENT ON COLUMN positions.stt IS 'Securities Transaction Tax';
COMMENT ON COLUMN positions.exchange_charges IS 'Exchange transaction charges';
COMMENT ON COLUMN positions.gst IS 'GST on brokerage + charges';
COMMENT ON COLUMN positions.net_pnl IS 'Net P&L (total_pnl - total_charges)';

-- Create index for net_pnl queries (most traders care about net P&L)
CREATE INDEX IF NOT EXISTS idx_positions_net_pnl ON positions(net_pnl);

-- Backfill existing positions with zero charges (will be calculated on next update)
-- This ensures existing data is not NULL
UPDATE positions
SET total_charges = 0,
    brokerage = 0,
    stt = 0,
    exchange_charges = 0,
    gst = 0,
    net_pnl = total_pnl  -- For now, set net = gross (will be recalculated)
WHERE total_charges IS NULL;
