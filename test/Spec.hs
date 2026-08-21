-- | Regression tests for the `bucket` and `arrayAt` operators.
--
--   Layers:
--     1. Unit tests of bucket/arrayAt semantics (incl. negative paths that MUST
--        throw -- empty/non-ascending breaks, out-of-range index).
--     2. Full equivalence sweep proving the compact grid encoding of the
--        congestion-charge config evaluates identically to the original
--        nested-if/else encoding for every (qar, distance, rain, serviceTier)
--        input, under the real engine via the shared JsonLogic.Runner.runLogics.
--     3. Grid shape check: rows == distBreaks+1, cols == qarBreaks+1.
module Main (main) where

import Control.Monad (forM_, when)
import Data.Aeson (Value (..), object, (.=))
import qualified Data.Aeson as A
import qualified Data.Aeson.KeyMap as AKM
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import Data.Scientific (toRealFloat)
import qualified Data.Vector as V
import JsonLogic (jsonLogicEither)
import JsonLogic.Runner (runLogics)
import System.Exit (exitFailure)
import Prelude

num :: Double -> Value
num = A.toJSON

-- tolerant deep comparison (numbers compared with epsilon)
approxEq :: Value -> Value -> Bool
approxEq (Object a) (Object b) =
  AKM.keys a == AKM.keys b
    && all (\k -> maybe False id (approxEq <$> AKM.lookup k a <*> AKM.lookup k b)) (AKM.keys a)
approxEq (Array a) (Array b) =
  V.length a == V.length b && and (V.zipWith approxEq a b)
approxEq (Number a) (Number b) = abs (toRealFloat a - toRealFloat b) < (1e-9 :: Double)
approxEq a b = a == b

throwsOn :: Value -> Bool
throwsOn expr = either (const True) (const False) (jsonLogicEither expr Null)

main :: IO ()
main = do
  failures <- newIORef (0 :: Int)
  let fail' msg = modifyIORef' failures (+ 1) >> putStrLn ("  FAIL: " <> msg)
      check name cond = if cond then putStrLn ("  ok: " <> name) else fail' name
      -- positive paths: extract the value (a throw would surface as Null and fail the check)
      evalR logic dat = either (const Null) id (jsonLogicEither logic dat)

  putStrLn "== unit: bucket (half-open [lo,hi)) =="
  let bucket v bs = evalR (object ["bucket" .= [num v, A.toJSON (bs :: [Double])]]) Null
  check "bucket 25 [30,40] = 0" (bucket 25 [30, 40] `approxEq` num 0)
  check "bucket 30 [30,40] = 1" (bucket 30 [30, 40] `approxEq` num 1) -- boundary -> upper slab
  check "bucket 35 [30,40] = 1" (bucket 35 [30, 40] `approxEq` num 1)
  check "bucket 40 [30,40] = 2" (bucket 40 [30, 40] `approxEq` num 2)
  check "bucket 99 [30,40] = 2" (bucket 99 [30, 40] `approxEq` num 2)

  putStrLn "== unit: bucket invalid input MUST throw =="
  check "empty breaks throws" (throwsOn (object ["bucket" .= [num 5, A.toJSON ([] :: [Double])]]))
  check "non-ascending breaks throws" (throwsOn (object ["bucket" .= [num 5, A.toJSON ([40, 30] :: [Double])]]))
  check "duplicate breaks throws" (throwsOn (object ["bucket" .= [num 5, A.toJSON ([30, 30, 40] :: [Double])]]))
  check "non-numeric value throws" (throwsOn (object ["bucket" .= [A.toJSON ("x" :: String), A.toJSON ([30] :: [Double])]]))
  check "NaN value (0/0) throws" (throwsOn (object ["bucket" .= [object ["/" .= [num 0, num 0]], A.toJSON ([30] :: [Double])]]))

  putStrLn "== unit: arrayAt =="
  let arr3 = A.toJSON ([10, 20, 30] :: [Double])
      arrayAt a i = evalR (object ["arrayAt" .= [a, num i]]) Null
  check "arrayAt [10,20,30] 0 = 10" (arrayAt arr3 0 `approxEq` num 10)
  check "arrayAt [10,20,30] 2 = 30" (arrayAt arr3 2 `approxEq` num 30)

  putStrLn "== unit: arrayAt out-of-range MUST throw (no clamp) =="
  check "index past end throws" (throwsOn (object ["arrayAt" .= [arr3, num 3]]))
  check "negative index throws" (throwsOn (object ["arrayAt" .= [arr3, num (-1)]]))

  putStrLn "== unit: nested grid lookup =="
  -- grid[dist][qar] via arrayAt(arrayAt(grid, bucket dist), bucket qar)
  let grid = A.toJSON ([[1.3, 1.0], [1.4, 1.0]] :: [[Double]])
      gridExpr =
        object
          [ "arrayAt"
              .= [ object ["arrayAt" .= [grid, object ["bucket" .= [object ["var" .= ("d" :: String)], A.toJSON [2 :: Double]]]]],
                   object ["bucket" .= [object ["var" .= ("q" :: String)], A.toJSON [30 :: Double]]]
                 ]
          ]
      at q d = evalR gridExpr (object ["q" .= (q :: Double), "d" .= (d :: Double)])
  check "grid q=15 d=1 = 1.3" (at 15 1 `approxEq` num 1.3)
  check "grid q=15 d=3 = 1.4" (at 15 3 `approxEq` num 1.4)
  check "grid q=40 d=1 = 1.0" (at 40 1 `approxEq` num 1.0)

  putStrLn "== nested object / array data literals: ALL keys preserved =="
  let multiKey = object ["a" .= num 1, "b" .= num 2, "c" .= num 3]
  check "multi-key data object keeps all keys" (evalR multiKey Null `approxEq` multiKey)

  let nestedObj = object ["wrapper" .= object ["x" .= num 1, "y" .= num 2, "z" .= num 3]]
  check "nested multi-key object value preserved" (evalR nestedObj Null `approxEq` nestedObj)

  let singleKey = object ["foo" .= num 1]
  check "single-key data object unchanged" (evalR singleKey Null `approxEq` singleKey)

  let nestedArr =
        object
          [ "arr" .= A.toJSON ([1, 2, 3] :: [Double]),
            "grid" .= A.toJSON ([[1, 2], [3, 4]] :: [[Double]]),
            "meta" .= object ["k" .= num 7, "v" .= num 8]
          ]
  check "nested arrays + object in data literal preserved" (evalR nestedArr Null `approxEq` nestedArr)

  let evalField = object ["result" .= object ["+" .= A.toJSON ([1, 2] :: [Double])], "keep" .= num 9]
      evalFieldExpected = object ["result" .= num 3, "keep" .= num 9]
  check "operator expr inside data field is evaluated" (evalR evalField Null `approxEq` evalFieldExpected)

  let varWhole = object ["var" .= ("obj" :: String)]
      varData = object ["obj" .= object ["a" .= num 1, "b" .= num 2]]
  check "var returns full multi-key object" (evalR varWhole varData `approxEq` object ["a" .= num 1, "b" .= num 2])

  putStrLn "== generalized nesting: depth, arrays-of-objects, empty object =="
  -- Deep (3-level) multi-key nesting: collapse must not lurk at any inner level.
  let deep =
        object
          [ "l1a" .= num 1,
            "l1b"
              .= object
                [ "l2a" .= num 2,
                  "l2b" .= object ["l3a" .= num 3, "l3b" .= num 4, "l3c" .= num 5]
                ]
          ]
  check "3-level multi-key nesting fully preserved" (evalR deep Null `approxEq` deep)

  let arrOfObjs = A.toJSON [object ["a" .= num 1, "b" .= num 2], object ["c" .= num 3, "d" .= num 4]]
  check "array of multi-key objects preserved" (evalR arrOfObjs Null `approxEq` arrOfObjs)

  -- Multi-key object nested inside an object field that is itself an array.
  let objArrObj = object ["rows" .= A.toJSON [object ["x" .= num 1, "y" .= num 2]], "n" .= num 1]
  check "object -> array -> multi-key object preserved" (evalR objArrObj Null `approxEq` objArrObj)

  check "empty object evaluates to Null" (evalR (object []) Null `approxEq` Null)

  putStrLn "== documented ambiguity: data object whose FIRST key is an operator name =="
  let ambiguous = object ["in" .= num 1, "zzz" .= num 2] -- keys sorted: "in" first
  check "operator-named first key is (mis)read as operator [known caveat]" (evalR ambiguous Null `approxEq` Null)

  putStrLn "== nested-object patch via cat/deepMerge (the version-26 regression) =="
  let baseConfig =
        object
          [ "pickupStallMonitoringConfig"
              .= object
                ["badTickDebounce" .= num 2, "gracePeriodSec" .= num 120, "progressThresholdMeters" .= num 50, "tickIntervalSec" .= num 60]
          ]
      newInner =
        object
          ["badTickDebounce" .= num 2, "gracePeriodSec" .= num 120, "progressThresholdMeters" .= num 55, "tickIntervalSec" .= num 60]
      patch =
        object
          [ "cat"
              .= [ object ["var" .= ("" :: String)],
                   object ["pickupStallMonitoringConfig" .= Null],
                   object ["pickupStallMonitoringConfig" .= newInner]
                 ]
          ]
      patched = evalR patch baseConfig
      expected = object ["pickupStallMonitoringConfig" .= newInner]
  check "nested-object patch applies (leaf 55, siblings preserved)" (patched `approxEq` expected)

  putStrLn "== equivalence sweep: v2 config (old if/else vs new bucket/arrayAt) =="
  equivalenceSweep failures "test/fixtures/congestion_old.json" "test/fixtures/congestion_new.json"

  putStrLn "== equivalence sweep: 250 config (old if/else vs new bucket/arrayAt) =="
  equivalenceSweep failures "test/fixtures/congestion250_old.json" "test/fixtures/congestion250_new.json"

  putStrLn "== grid shape check (rows == distBreaks+1, cols == qarBreaks+1) =="
  forM_ ["test/fixtures/congestion_new.json", "test/fixtures/congestion250_new.json"] $ \fp -> do
    raw <- BL.readFile fp
    case A.decode raw of
      Just v -> case gridShapeErrors v of
        [] -> putStrLn ("  ok: " <> fp <> " grid dimensions consistent")
        errs -> mapM_ (\e -> fail' (fp <> ": " <> e)) errs
      Nothing -> fail' ("could not decode " <> fp)

  n <- readIORef failures
  if n == 0
    then putStrLn "\nALL TESTS PASSED"
    else putStrLn ("\n" <> show n <> " FAILURES") >> exitFailure

-- | Run the full input space through both configs and assert identical output.
--   rainStatus is swept over the known states plus Nothing (field omitted),
--   the shape of the 250.json verification inputs. Uses the shared runLogics so
--   the test can't drift from the engine/jl-verify on error semantics; any rule
--   error in either config also counts as a mismatch.
equivalenceSweep :: IORef Int -> FilePath -> FilePath -> IO ()
equivalenceSweep failures oldPath newPath = do
  oldRaw <- BL.readFile oldPath
  newRaw <- BL.readFile newPath
  case (A.decode oldRaw, A.decode newRaw) of
    (Just (Array oldA), Just (Array newA)) -> do
      let oldLogics = V.toList oldA
          newLogics = V.toList newA
          tiers = ["TAXI", "AUTO_RICKSHAW", "ECO", "COMFY", "SUV", "SUV_PLUS", "AUTO_PLUS"]
          rains = [Just "no_rain", Just "light_rain", Just "heavy_rain", Just "no_data", Just "drizzle", Nothing]
          qars = [fromIntegral i * 0.05 | i <- [0 .. 17 :: Int]] :: [Double] -- actualQAR -> qar 0..85
          dists = [fromIntegral d | d <- [0 .. 30 :: Int]] <> [2, 4, 6, 8, 10, 12, 16, 20, 19.999] :: [Double]
      mism <- newIORef (0 :: Int)
      total <- newIORef (0 :: Int)
      forM_ tiers $ \t ->
        forM_ rains $ \r ->
          forM_ qars $ \aq ->
            forM_ dists $ \d -> do
              let dat =
                    object $
                      [ "actualQAR" .= (aq :: Double),
                        "distanceInKm" .= d,
                        "serviceTier" .= (t :: String)
                      ]
                        <> maybe [] (\rr -> ["rainStatus" .= (rr :: String)]) r
                  (resOld, errOld) = runLogics oldLogics dat
                  (resNew, errNew) = runLogics newLogics dat
              modifyIORef' total (+ 1)
              if approxEq resOld resNew && null errOld && null errNew
                then pure ()
                else do
                  m <- readIORef mism
                  when (m < 10) $
                    putStrLn
                      ( "  DIFF " <> t <> "/" <> show r <> " qar=" <> show (aq * 100) <> " d=" <> show d
                          <> (if null (errOld <> errNew) then "" else " errs=" <> show (errOld <> errNew))
                      )
                  modifyIORef' mism (+ 1)
                  modifyIORef' failures (+ 1)
      m <- readIORef mism
      tot <- readIORef total
      putStrLn ("  inputs tested: " <> show tot)
      if m == 0
        then putStrLn "  ok: ZERO mismatches across all tiers x rain x qar x distance"
        else putStrLn ("  FAIL: " <> show m <> " mismatches")
    _ -> do
      modifyIORef' failures (+ 1)
      putStrLn "  FAIL: fixtures must be JSON arrays of rules"

-- | Walk a config and, for every grid lookup of the shape
--     arrayAt[ arrayAt[ GRID, bucket[_, distBreaks] ], bucket[_, qarBreaks] ],
--   assert GRID has (distBreaks+1) rows and every row has (qarBreaks+1) columns.
--   Belt-and-suspenders alongside arrayAt's fail-loud: catches a resized-breaks /
--   unresized-grid mistake at config-review time for the committed fixtures.
gridShapeErrors :: Value -> [String]
gridShapeErrors val = case val of
  Object o -> here o <> concatMap gridShapeErrors (AKM.elems o)
  Array a -> concatMap gridShapeErrors (V.toList a)
  _ -> []
  where
    breaksLen v = case v of
      Object b -> case AKM.lookup "bucket" b of
        Just (Array bs) | V.length bs == 2 -> case bs V.! 1 of
          Array brk -> Just (V.length brk)
          _ -> Nothing
        _ -> Nothing
      _ -> Nothing
    here o = case AKM.lookup "arrayAt" o of
      Just (Array outer)
        | V.length outer == 2,
          Object innerO <- outer V.! 0,
          Just nQar <- breaksLen (outer V.! 1),
          Just (Array innerArgs) <- AKM.lookup "arrayAt" innerO,
          V.length innerArgs == 2,
          Array grid <- innerArgs V.! 0,
          Just nDist <- breaksLen (innerArgs V.! 1) ->
          let rows = V.length grid
              rowErr = ["grid has " <> show rows <> " rows, expected distBreaks+1 = " <> show (nDist + 1) | rows /= nDist + 1]
              colErrs =
                [ "grid row " <> show i <> " has " <> show (V.length r) <> " cols, expected qarBreaks+1 = " <> show (nQar + 1)
                  | (i, Array r) <- zip [0 :: Int ..] (V.toList grid),
                    V.length r /= nQar + 1
                ]
           in rowErr <> colErrs
      _ -> []
