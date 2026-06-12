-- | Local equivalent of control-center's dynamic-logic "verify".
--
--   It runs the SAME engine call the backend uses (JsonLogic.runLogics, which
--   mirrors Lib.Yudhishthira.Tools.Utils.runLogics), so the output is exactly
--   what /config/dynamic-logic would show for the same logic + data.
--
--   Usage:
--     jl-verify <config.json> '<inputJSON>'
--     jl-verify <config.json> @inputFile.json
--
--   <config.json> is the dynamic-logic array of rules.
--   <inputJSON> is the "verification data" object, e.g.
--     {"actualQAR":0.45,"distanceInKm":3,"rainStatus":"no_rain","serviceTier":"TAXI"}
module Main (main) where

import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.List (isPrefixOf)
import qualified Data.Vector as V
import JsonLogic (runLogics)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import Prelude

main :: IO ()
main = do
  args <- getArgs
  case args of
    [cfgPath, inputArg] -> do
      cfgRaw <- BL.readFile cfgPath
      inputRaw <-
        if "@" `isPrefixOf` inputArg
          then BL.readFile (drop 1 inputArg)
          else pure (BLC.pack inputArg)
      case (A.eitherDecode cfgRaw, A.eitherDecode inputRaw) of
        (Right (A.Array rules), Right input) -> do
          let (result, errs) = runLogics (V.toList rules) input
          putStrLn "── input ──────────────────────────────────────────"
          BLC.putStrLn (A.encode input)
          -- Make a partial evaluation impossible to scan past: if any rule
          -- failed, the result is the value threaded through *up to* the
          -- failure, not a clean answer.
          if null errs
            then putStrLn "── result (runLogics output) ──────────────────────"
            else putStrLn "── result (PARTIAL — some rules failed, see errors) ─"
          BLC.putStrLn (A.encode result)
          if null errs
            then putStrLn "── errors ── none"
            else do
              putStrLn "── errors ──"
              mapM_ (putStrLn . ("  " <>)) errs
              exitFailure
        (Left e, _) -> putStrLn ("config decode error: " <> e) >> exitFailure
        (_, Left e) -> putStrLn ("input decode error: " <> e) >> exitFailure
        (Right _, Right _) -> putStrLn "config must be a JSON array of rules" >> exitFailure
    _ -> do
      putStrLn "usage: jl-verify <config.json> '<inputJSON>'   (or @inputFile.json)"
      exitFailure
