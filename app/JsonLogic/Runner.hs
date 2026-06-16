-- | Sequential rule runner, kept in its own module so it is NOT re-exported by
--   the widely-imported `JsonLogic` module. The nammayatri backend imports
--   `JsonLogic` unqualified and already has its own `runLogics` in
--   `Lib.Yudhishthira.Tools.Utils`; exporting one from `JsonLogic` would make
--   the name ambiguous there. Tests and `jl-verify` import this module directly.
module JsonLogic.Runner (runLogics) where

import Data.Aeson (Value)
import JsonLogic (jsonLogicEither)
import Prelude

-- | Fold the engine over a list of logic rules, threading the accumulated
--   result forward. Mirrors the backend's
--   @Lib.Yudhishthira.Tools.Utils.runLogics@: a rule that throws is recorded
--   (tagged with its 0-based index) and evaluation continues from the previous
--   accumulator. Returns the final accumulator and the collected errors so a
--   caller can show "value threaded through, but rule N failed" rather than
--   silently dropping the error.
runLogics :: [Value] -> Value -> (Value, [String])
runLogics logics dat = go (0 :: Int) dat [] logics
  where
    go _ acc errs [] = (acc, reverse errs)
    go i acc errs (l : ls) = case jsonLogicEither l acc of
      Left e -> go (i + 1) acc (("rule " <> show i <> ": " <> show e) : errs) ls
      Right r -> go (i + 1) r errs ls
