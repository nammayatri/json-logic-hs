{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module JsonLogic where

import Control.Concurrent.Async (mapConcurrently)
import Control.Monad (liftM2, unless, when)
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Either (left, right, runEitherT)
import Data.Aeson as A
import qualified Data.Aeson.Key as AK
import qualified Data.Aeson.KeyMap as AKM
import qualified Data.Aeson.QQ.Simple as AQ
import Data.Foldable (foldlM)
import Data.Int (Int64)
import qualified Data.List as DL
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, listToMaybe, mapMaybe)
import Data.Scientific (toBoundedInteger, toRealFloat)
import qualified Data.Text as DT
import qualified Data.Text.Read as TR
import qualified Data.Tuple.Extra as DTE
import qualified Data.Vector as V
import Debug.Trace (traceShowId)
import System.IO.Unsafe (unsafePerformIO)
import Text.Read (readMaybe)
import Prelude

newtype JsonLogicError = JsonLogicError
  { errorMessage :: String
  }
  deriving (Eq, Show)

instance Exception JsonLogicError

_poolingTest :: (MonadThrow m, MonadCatch m, MonadIO m) => m ()
_poolingTest = do
  let tests = [AQ.aesonQQ|{ "drop": [ {"var": "drivers"}, 5 ] }|]
  let data_ = [AQ.aesonQQ|{ "drivers":  "POST_driverName" }|]

  let tests2 = [AQ.aesonQQ|{ "drop": [ {"var": "drivers"}, 5 ] }|]
  let data2_ = [AQ.aesonQQ|{ "drivers":  [1, 2, 3, 4, 5, 6, 7] }|]

  let tests3 = [AQ.aesonQQ|{ "min": [ {"var": "drivers"} ] }|]
  let data3_ = [AQ.aesonQQ|{ "drivers":  [[[1, 2, 3, -4, 5, 6, 7] ,[1, 2, 3, 4, 5, 6, 7 ]], [1, -2, 3, 4, 5, 6, 10], [-1, 2, 3, 4, 5, 6, 11]] }|]

  let tests4 = [AQ.aesonQQ|{ "map": [ {"var": "drivers"}, { "+" : [{"var": "A"}, 10]} ] }|]
  let data4_ = [AQ.aesonQQ|{ "drivers":  [ { "A": 1 }, { "A": 2 }, { "A": 3 } ] }|]

  let tests5 = [AQ.aesonQQ|{ "map'": [ {"var": "drivers"}, { "+" : [{"var": "A"}, 10]} ] }|]
  let data5_ = [AQ.aesonQQ|{ "drivers":  [ { "A": 1 }, { "A": 2 }, { "A": 3 } ] }|]

  let tests6 = [AQ.aesonQQ|{ "fold": [ {"var": "drivers.a.a"}, 0, { "+" : [{"var": "A"}]} ] }|]
  let data6_ = [AQ.aesonQQ|{ "drivers":  {"a": { "a": [ { "A": 1 }, { "A": 2 }, { "A": 3 } ] } }}|]

  let tests7 = [AQ.aesonQQ|{ "fold'": [ {"var": "drivers.a.a"}, 0, { "+" : [{"var": "A"}]} ] }|]
  let data7_ = [AQ.aesonQQ|{ "drivers":  {"a": { "a": [ { "A": 1 }, { "A": 2 }, { "A": 3 } ] } }}|]

  let tests8 = [AQ.aesonQQ|{ "on": [ {"var": "drivers.a"}, { "fold'": [ {"var": "a"}, 0, { "+" : [{"var": "A"}]} ] }] }|]
  let data8_ = [AQ.aesonQQ|{ "drivers":  {"a": { "a": [ { "A": 1 }, { "A": 2 }, { "A": 3 } ] } }}|]

  liftIO . print $ jsonLogicEither tests data_
  liftIO . print $ jsonLogicEither tests2 data2_
  liftIO . print $ jsonLogicEither tests3 data3_
  liftIO . print . A.encode =<< jsonLogic tests4 data4_
  liftIO . print . A.encode =<< jsonLogic tests5 data5_
  liftIO . print . A.encode =<< jsonLogic tests6 data6_
  liftIO . print . A.encode =<< jsonLogic tests7 data7_
  liftIO . print . A.encode =<< jsonLogic tests8 data8_

_test :: (MonadThrow m, MonadCatch m, MonadIO m) => m ()
_test = do
  let tests = [AQ.aesonQQ|{ "filter": [ { "var": "drivers" }, { "!=": [ { "var": "driverPoolResult.driverTags.SafetyCohort" }, "Unsafe" ] } ] }|]
  let data_ = [AQ.aesonQQ|{ "drivers": [{"driverPoolResult": { "driverTags": { "SafetyCohort": "Unsafe"}, "c": 1 } }, {"driverPoolResult": { "driverTags": { "SafetyCohort": "Safe"}, "c": 1 } }] }|]
  liftIO . print . A.encode =<< jsonLogic tests data_

jsonLogicEitherIO :: (MonadCatch m, MonadIO m) => Value -> Value -> m (Either SomeException Value)
jsonLogicEitherIO logic data_ = runEitherT $ do
  result <- lift (jsonLogic logic data_) `catch` left
  right result

jsonLogicEither :: Value -> Value -> Either SomeException Value
jsonLogicEither logic data_ = unsafePerformIO (jsonLogicEitherIO logic data_)
{-# NOINLINE jsonLogicEither #-}

-- | Fold the engine over a list of logic rules, threading the accumulated
--   result forward. This mirrors the backend's
--   @Lib.Yudhishthira.Tools.Utils.runLogics@: a rule that throws is recorded
--   (tagged with its 0-based index) and evaluation continues from the previous
--   accumulator. Returns the final accumulator and the collected errors so
--   callers can present "value threaded through, but rule N failed" rather than
--   silently dropping the error. Shared so the engine, tests and jl-verify can't
--   drift apart on this semantics.
runLogics :: [Value] -> Value -> (Value, [String])
runLogics logics dat = go (0 :: Int) dat [] logics
  where
    go _ acc errs [] = (acc, reverse errs)
    go i acc errs (l : ls) = case jsonLogicEither l acc of
      Left e -> go (i + 1) acc (("rule " <> show i <> ": " <> show e) : errs) ls
      Right r -> go (i + 1) r errs ls

jsonLogic :: (MonadThrow m, MonadCatch m, MonadIO m) => Value -> Value -> m Value
jsonLogic tests data_ =
  case tests of
    A.Object dict ->
      case AKM.toList dict of
        [] -> pure A.Null
        ((k, v) : _) ->
          let operator = AK.toString k
              values = v
              eatTheKey = DL.isSuffixOf "'" operator
              operator' = AK.fromString $ if eatTheKey then DT.unpack $ DT.replace "'" "" (DT.pack operator) else operator
           in applyOperation' operator' values eatTheKey
    A.Array rules -> A.Array <$> V.mapM (`jsonLogic` data_) rules
    _ -> pure tests
  where
    isLogicOperation op = op `elem` Map.keys (operations :: Map.Map Key ([Value] -> IO Value))
    applyOperation' "filter" (A.Array values) eatTheKey = applyOperation "filter" (V.toList values) data_ eatTheKey
    applyOperation' "sort" (A.Array values) eatTheKey = applyOperation "sort" (V.toList values) data_ eatTheKey
    applyOperation' "map" (A.Array values) eatTheKey = applyOperation "map" (V.toList values) data_ eatTheKey
    applyOperation' "fold" (A.Array values) eatTheKey = applyOperation "fold" (V.toList values) data_ eatTheKey
    applyOperation' "on" (A.Array values) _ = applyOperation "on" (V.toList values) data_ False
    applyOperation' "switch" (A.Array values) _ = applyOperation "switch" (V.toList values) data_ False
    applyOperation' "var" values eatTheKey = do
      resolvedValues <- jsonLogicValues values data_
      applyOperation "var" resolvedValues data_ eatTheKey
    applyOperation' operator values eatTheKey = do
      if isLogicOperation operator
        then do
          resolvedValues <- jsonLogicValues values data_
          applyOperation operator resolvedValues data_ eatTheKey
        else do
          resolvedValues <- jsonLogic values data_
          pure $ A.object [operator .= resolvedValues]

applyOperation :: (MonadThrow m, MonadCatch m, MonadIO m) => Key -> [Value] -> Value -> Bool -> m Value
applyOperation "var" [A.Number ind] (A.Array arr) _ =
  pure $ fromMaybe A.Null $ arr V.!? (fromMaybe 0 $ toBoundedInteger ind :: Int)
applyOperation "var" [A.String ind] (A.Array arr) _ =
  pure $ fromMaybe A.Null $ arr V.!? (fromMaybe 0 $ readMaybe (DT.unpack ind) :: Int)
applyOperation "var" [A.String ""] data_ _ = pure $ getVar data_ "" data_
applyOperation "var" [A.String var] data_ _ = pure $ getVar data_ var A.Null
applyOperation "var" [A.String var, value] data_ _ = pure $ getVar data_ var value
applyOperation "var" [A.Null] A.Null _ = pure $ A.Number 1
applyOperation "var" [] A.Null _ = pure $ A.Number 1
applyOperation "var" [A.Null] data_ _ = pure data_
applyOperation "var" [] data_ _ = pure data_
applyOperation "var" _ _ _ = throwM $ JsonLogicError ("Wrong number of arguments for var" :: String)
applyOperation "sort" values data_ eatTheKey = sortValues values data_ eatTheKey
applyOperation "map" values data_ eatTheKey = mapIt values data_ eatTheKey
applyOperation "on" [A.Object listDataVar, operationObject] data_ _ = do
  case AKM.lookup (AK.fromString "var") listDataVar of
    Just (A.String varFromData) -> do
      updatedData <- jsonLogic operationObject (getVar data_ varFromData A.Null)
      putVar varFromData updatedData data_
    _ -> throwM $ JsonLogicError ("var must be specified here" :: String)
applyOperation "fold" [A.Object listDataVar, initial, A.Object operationObject] data_ eatTheKey = do
  case AKM.lookup (AK.fromString "var") listDataVar of
    Just (A.String varFromData) ->
      case getVar data_ varFromData A.Null of
        A.Array (listToFold :: V.Vector Value) -> do
          let operation = listToMaybe $ AKM.keys operationObject
          let operands = listToMaybe $ AKM.elems operationObject
          case (operation, operands) of
            (Just operation', Just (A.Array operands')) -> do
              case V.toList operands' of
                (operands'' : xs) -> do
                  updatedData <-
                    V.foldM
                      ( \acc x -> do
                          case operands'' of
                            A.Object varThing -> do
                              case AKM.lookup (AK.fromString "var") varThing of
                                Just (A.String varFromData') -> do
                                  jsonLogic (A.Object (AKM.singleton operation' (A.toJSON [acc, getVar x varFromData' A.Null]))) A.Null
                                _ -> throwM $ JsonLogicError ("var must be specified here" :: String)
                            _ -> throwM $ JsonLogicError ("wrong type of operands passed for folding" :: String)
                      )
                      initial
                      (listToFold <> V.fromList xs)
                  if eatTheKey then pure updatedData else putVar varFromData updatedData data_
                _ -> throwM $ JsonLogicError ("wrong type of operands passed for folding" :: String)
            _ -> throwM $ JsonLogicError ("wrong type of operation or operands passed for folding" :: String)
        _ -> throwM $ JsonLogicError ("wrong type of variable passed for folding 1" :: String)
    _ -> throwM $ JsonLogicError ("var must be specified here" :: String)
applyOperation "filter" [A.Object var, operation] data_ eatTheKey = do
  case AKM.lookup (AK.fromString "var") var of
    Just (A.String varFromData) ->
      case getVar data_ varFromData A.Null of
        A.Array listToFilter -> do
          updatedData <- A.Array <$> V.filterM (fmap (not . toBool) . jsonLogic operation) listToFilter
          if eatTheKey then pure updatedData else putVar varFromData updatedData data_
        _ -> throwM $ JsonLogicError ("wrong type of variable passed for filtering" :: String)
    _ -> throwM $ JsonLogicError ("var must be specified here" :: String)
applyOperation "switch" [keyExpr, casesObj, defaultVal] data_ _ = do
  key <- jsonLogic keyExpr data_
  case (key, casesObj) of
    (A.String s, A.Object cases) ->
      case AKM.lookup (AK.fromText s) cases of
        Just matched -> jsonLogic matched data_
        Nothing -> jsonLogic defaultVal data_
    (_, A.Object _) -> jsonLogic defaultVal data_
    _ -> throwM $ JsonLogicError ("switch: 2nd argument must be an object of cases" :: String)
applyOperation "switch" [keyExpr, casesObj] data_ _ = applyOperation "switch" [keyExpr, casesObj, A.Null] data_ False
applyOperation "switch" _ _ _ = throwM $ JsonLogicError ("switch: wrong number of arguments" :: String)
-- applyOperation "missing" (A.Array args) data_ = List $ missing data_ args TODO: add these if required later
-- applyOperation "missing_some" [Num minReq, List args] data_ = List $ missingSome data_ (round minReq) args
applyOperation op args _ _ = do
  case Map.lookup op operations of
    Just fn -> fn args
    Nothing -> pure A.Null

mapIt :: (MonadThrow m, MonadCatch m, MonadIO m) => [Value] -> Value -> Bool -> m Value
mapIt [A.String mapOn, operation] data_ eatTheKey = mapIt' mapOn operation data_ eatTheKey
mapIt [A.Object mapOnVar, operation] data_ eatTheKey =
  case AKM.lookup (AK.fromString "var") mapOnVar of
    Just (A.String varFromData) -> mapIt' varFromData operation data_ eatTheKey
    _ -> throwM $ JsonLogicError ("var must be specified here" :: String)
mapIt _ _ _ = throwM $ JsonLogicError ("var must be specified here" :: String)

mapIt' :: (MonadThrow m, MonadCatch m, MonadIO m) => DT.Text -> Value -> Value -> Bool -> m Value
mapIt' mapOn operation data_ eatTheKey =
  case getVar data_ mapOn A.Null of
    A.Array listToFilter -> do
      updatedData <- A.Array <$> liftIO (mapConcurrently (jsonLogic operation) listToFilter)
      if eatTheKey then pure updatedData else putVar mapOn updatedData data_
    _ -> throwM $ JsonLogicError ("wrong type of variable passed for mapping" :: String)

sortValues :: (MonadThrow m, MonadCatch m, MonadIO m) => [Value] -> Value -> Bool -> m Value
sortValues [] _ _ = throwM $ JsonLogicError ("wrong usage of sort command" :: String)
sortValues (x : restValues) data_ eatTheKey = go x
  where
    go (A.Object sortWhat) = do
      case AKM.lookup (AK.fromString "var") sortWhat of
        Just (A.String varFromData) ->
          case getVar data_ varFromData A.Null of
            A.Array listToSort -> do
              updatedVar <- A.Array . V.fromList <$> sortValues' restValues (V.toList listToSort)
              if eatTheKey then pure updatedVar else putVar varFromData updatedVar data_
            _ -> throwM $ JsonLogicError ("wrong type of variable passed for sorting" :: String)
        _ -> throwM $ JsonLogicError ("wrong type of variable passed for sorting" :: String)
    go (A.String varFromData) = do
      case getVar data_ varFromData A.Null of
        A.Array listToSort -> do
          updatedVar <- A.Array . V.fromList <$> sortValues' restValues (V.toList listToSort)
          if eatTheKey then pure updatedVar else putVar varFromData updatedVar data_
        _ -> throwM $ JsonLogicError ("Wrong type of variable passed for sorting" :: String)
    go _ = throwM $ JsonLogicError ("cannot figureout what to sort broo" :: String)

sortValues' :: (MonadThrow m, MonadCatch m, MonadIO m) => [Value] -> [Value] -> m [Value]
sortValues' [] listToSort = pure $ map A.Object . DL.sort $ mapMaybe getObjects listToSort
sortValues' [A.String on] listToSort = pure $ map A.Object . sortValuesOn on $ mapMaybe getObjects listToSort
sortValues' [A.Object on] listToSort = do
  case AKM.lookup (AK.fromString "var") on of
    Just (A.String on') -> pure $ map A.Object . sortValuesOn on' $ mapMaybe getObjects listToSort
    _ -> throwM $ JsonLogicError ("on part of sort command contains unsupported type for var field, should be string, on command: " <> show on :: String)
sortValues' on _ = throwM $ JsonLogicError ("wrong type of data used to pass 'on' field for sortOn, should be String or {\"var\":\"String\"}, on command: " <> show on :: String)

getObjects :: Value -> Maybe (AKM.KeyMap Value)
getObjects (A.Object obj) = Just obj
getObjects _ = Nothing

sortValuesOn :: DT.Text -> [AKM.KeyMap Value] -> [AKM.KeyMap Value]
sortValuesOn on = do
  DL.sortOn (\element -> getVar (A.Object element) on A.Null)

parseInt :: DT.Text -> Either String Int
parseInt t =
  case TR.decimal t of
    Right (n, leftover) | DT.null leftover -> Right n
    _ -> Left "invalid int"

getVar :: Value -> DT.Text -> Value -> Value
getVar a "" _ = a
getVar (A.Array arr) varName notFound = do
  case parseInt varName of
    Left _ -> notFound
    Right index -> case V.indexM arr index of
      Just res -> res
      Nothing -> notFound
getVar (A.Object dict) varName notFound = do
  getVarHelper dict (DT.split (== '.') varName)
  where
    getVarHelper d [key] =
      case (AKM.lookup (AK.fromString $ DT.unpack key) d) of
        Just res -> res
        Nothing -> notFound
    getVarHelper d (key : restKey) =
      case (AKM.lookup (AK.fromString $ DT.unpack key) d) of
        Just d' -> getVar d' (DT.intercalate "." restKey) notFound
        _ -> notFound
    getVarHelper _ _ = notFound
getVar _ _ notFound = notFound

putVar :: (MonadThrow m, MonadCatch m, MonadIO m) => DT.Text -> Value -> Value -> m Value
putVar key newVal (A.Object obj) = fmap A.Object $ putVarHelper obj (DT.split (== '.') key) newVal
  where
    putVarHelper :: (MonadThrow m, MonadCatch m, MonadIO m) => AKM.KeyMap Value -> [DT.Text] -> Value -> m (AKM.KeyMap Value)
    putVarHelper d [finalKey] val = pure $ AKM.insert (AK.fromString $ DT.unpack finalKey) val d
    putVarHelper d (key' : restKey) val =
      case AKM.lookup (AK.fromString $ DT.unpack key') d of
        Just (A.Object d') -> do
          updatedRes <- putVarHelper d' restKey val
          pure $ AKM.insert (AK.fromString $ DT.unpack key') (A.Object updatedRes) d
        _ -> throwM $ JsonLogicError ("Path does not exist for putting value" :: String)
    putVarHelper _ _ _ = throwM $ JsonLogicError ("Invalid path for putting value" :: String)
putVar _ _ _ = throwM $ JsonLogicError ("putVar expects an object as the target value" :: String)

jsonLogicValues :: (MonadThrow m, MonadCatch m, MonadIO m) => Value -> Value -> m [Value]
jsonLogicValues (Array vals) data_ = mapM (`jsonLogic` data_) $ V.toList vals
jsonLogicValues val data_ = do
  res <- jsonLogic val data_
  pure [res]

compareJsonImpl :: Ordering -> Value -> Value -> Bool
compareJsonImpl EQ a b = a == b
compareJsonImpl _ A.Null _ = False -- null is not orderable; comparisons involving null always return false
compareJsonImpl _ _ A.Null = False
compareJsonImpl ordering a b =
  ( case ordering of
      LT -> (<)
      GT -> (>)
  )
    a
    b

toBool :: Value -> Bool
toBool a = case a of
  A.Bool aa -> not aa
  A.Null -> True
  A.Array aa -> V.null aa
  A.String aa -> DT.null aa
  A.Number aa -> aa == 0
  A.Object _ -> False

modOperator :: (MonadThrow m, MonadCatch m, MonadIO m) => Value -> Value -> m Int64
modOperator a b =
  case (a, b) of
    (A.Number aa, A.Number bb) -> do
      let aaB = toBoundedInteger aa
          bbB = toBoundedInteger bb
      case (aaB, bbB) of
        (Just aaB', Just bbB') -> pure $ mod aaB' bbB'
        _ -> throwM $ JsonLogicError ("Couldn't parse numbers" :: String)
    _ -> throwM $ JsonLogicError ("Invalid input type for mod operator a: " <> show a <> ", b: " <> show b :: String)

ifOp :: (MonadThrow m, MonadCatch m, MonadIO m) => [Value] -> m Value
ifOp [a, b, c] = pure $ if not (toBool a) then b else c
ifOp [a, b] = pure $ if not (toBool a) then b else A.Null
ifOp [a] = pure $ if not (toBool a) then a else A.Bool False
ifOp [] = pure A.Null
ifOp args = throwM $ JsonLogicError ("wrong number of args supplied, need 3 or less" <> show args :: String)

coalesceOp :: (MonadThrow m, MonadCatch m, MonadIO m) => [Value] -> m Value
coalesceOp [] = pure A.Null
coalesceOp (A.Null : rest) = coalesceOp rest
coalesceOp (x : _) = pure x

unaryOp :: (MonadThrow m, MonadCatch m, MonadIO m) => (Value -> m a) -> [Value] -> m a
unaryOp fn [a] = fn a
unaryOp _ _ = throwM $ JsonLogicError ("wrong number of args supplied, need 1" :: String)

logValue :: Value -> Value
logValue = traceShowId

binaryOp :: forall m a. (MonadThrow m, MonadCatch m, MonadIO m) => (Value -> Value -> m a) -> [Value] -> m a
binaryOp fn [a, b] = fn a b
binaryOp _ _ = throwM $ JsonLogicError ("wrong number of args supplied, need 2" :: String)

inOp :: (MonadThrow m, MonadCatch m, MonadIO m) => Value -> Value -> m Bool
inOp a bx = case (a, bx) of
  (a', A.Array bx') -> pure $ V.elem a' bx'
  (A.String a', A.String bx') -> pure $ a' `DT.isInfixOf` bx'
  _ -> throwM $ JsonLogicError ("failed to check if " <> show a <> " is in " <> show bx :: String)

getNumber' :: (MonadThrow m, MonadCatch m, MonadIO m) => Value -> m Double
getNumber' a = case a of
  A.Number aa -> pure $ toRealFloat aa
  _ -> throwM $ JsonLogicError ("expected number, got -> " <> show a :: String)

--
operateNumber :: (MonadThrow m, MonadCatch m, MonadIO m) => (Double -> Double -> m Double) -> Double -> Value -> m Double
operateNumber fn acc a = do
  a' <- getNumber' a
  acc `fn` a'

concatValue :: Value -> Value -> Value
concatValue a b = deepMerge a b

deepMerge :: Value -> Value -> Value
deepMerge (Object a) (Object b) = Object (AKM.unionWith deepMerge a b)
deepMerge (Array a) (Array b) = Array (a <> b)
deepMerge a Null = a
deepMerge _ b = b

binaryOpJson :: forall m a. (MonadThrow m, MonadCatch m, MonadIO m) => ToJSON a => (Value -> Value -> m a) -> [Value] -> m Value
binaryOpJson fn = fmap A.toJSON . binaryOp fn

listOpJson :: (MonadThrow m, MonadCatch m, MonadIO m) => ToJSON a => (a -> Value -> m a) -> a -> [Value] -> m Value
listOpJson fn acc = fmap A.toJSON . listOp fn acc

operateNumberList :: (MonadThrow m, MonadCatch m, MonadIO m) => (Double -> Value -> m Double) -> (Double -> Double) -> [Value] -> m Value
operateNumberList _ _ [] = pure A.Null
operateNumberList fn onlyEntryAction ((A.Object varThing) : xs) = do
  case AKM.lookup (AK.fromString "var") varThing of
    Just (A.String varFromData) -> do
      let numberValList = map (\x -> getVar x varFromData A.Null) xs
      operateNumberList fn onlyEntryAction numberValList
    _ -> throwM $ JsonLogicError ("var must be specified here" :: String)
operateNumberList fn onlyEntryAction [xs] = do
  case xs of
    A.Array xs' -> operateNumberList fn onlyEntryAction $ V.toList xs'
    _ -> fmap (A.toJSON . onlyEntryAction) $ getNumber' xs
operateNumberList fn onlyEntryAction (x : xs) = do
  case x of
    A.Array x' -> do
      fstRes <- operateNumberList fn onlyEntryAction $ V.toList x'
      restResults <- mapM (\xs' -> operateNumberList fn onlyEntryAction [xs']) xs
      accNumber <- getNumber' fstRes
      fmap A.toJSON $ listOp fn accNumber restResults
    _ -> do
      accNumber <- getNumber' x
      fmap A.toJSON $ listOp fn accNumber xs

listOp :: (MonadThrow m, MonadCatch m, MonadIO m, A.ToJSON a) => (a -> Value -> m a) -> a -> [Value] -> m a
listOp _ acc [] = pure acc
listOp fn acc ((A.Object varThing) : xs) = do
  case AKM.lookup (AK.fromString "var") varThing of
    Just (A.String varFromData) -> do
      let listVal = map (\x -> getVar x varFromData A.Null) xs
      listOp fn acc listVal
    _ -> foldlM fn acc (A.Object varThing : xs)
listOp fn acc (x : xs) = do
  case x of
    A.Array xs' -> do
      results <- mapM (\xs'' -> fmap A.toJSON $ listOp fn acc [xs'']) xs'
      foldlM fn acc results
    _ -> foldlM fn acc (x : xs)

listOpWithOutAcc :: (MonadThrow m, MonadCatch m, MonadIO m) => (Value -> Value -> m Value) -> [Value] -> m Value
listOpWithOutAcc fn vals = go vals
  where
    go (A.Object _ : _) = listOp fn (A.Object AKM.empty) vals
    go _ = listOp fn (A.String "") vals

merge :: [Value] -> Value
merge = A.Array . V.fromList . concatMap getArr
  where
    getArr val = case val of
      A.Array arr -> V.toList arr
      e -> [e]

compareJson :: (MonadThrow m, MonadCatch m, MonadIO m) => Ordering -> [Value] -> m Bool
compareJson ord values = binaryOp (\a -> pure . compareJsonImpl ord a) values

compareAll :: (MonadThrow m, MonadCatch m, MonadIO m) => (Value -> Value -> Bool) -> [Value] -> m Bool
compareAll fn (x : y : xs) = liftM2 (&&) (pure $ fn x y) (compareAll fn (y : xs))
compareAll _ (_x : _xs) = pure True
compareAll _ _ = throwM $ JsonLogicError ("need atleast one element" :: String)

compareWithAll :: (MonadThrow m, MonadCatch m, MonadIO m) => Ordering -> [Value] -> m Bool -- TODO: probably could be improved, but wanted to write this way 😊
compareWithAll _ [] = pure False
compareWithAll _ [_x] = pure False
compareWithAll ordering xs = compareAll (compareJsonImpl ordering) xs

data TrimOp = Take | Drop deriving (Show)

listTrimmingOperators :: (MonadThrow m, MonadCatch m, MonadIO m) => TrimOp -> Value -> Value -> m Value
listTrimmingOperators trimOp arrToCut cutFrom = do
  fromIndex <- round <$> getNumber' cutFrom
  elementsFromIndex trimOp fromIndex arrToCut

elementsFromIndex :: (MonadThrow m, MonadCatch m, MonadIO m) => TrimOp -> Int -> Value -> m Value
elementsFromIndex trimOp n (String txt) =
  pure $ case trimOp of
    Drop -> String $ DT.drop n txt
    Take -> String $ DT.take n txt
elementsFromIndex trimOp n (Array arr) =
  pure $ case trimOp of
    Drop -> Array $ V.drop n arr
    Take -> Array $ V.take n arr
elementsFromIndex trimOp a b = throwM $ JsonLogicError ("wrong type of variable passed for substr " <> show (A.encode a) <> " " <> show (A.encode b) <> " op: " <> show trimOp :: String)

-- | bucket: given a value and a list of breakpoints, return the slab index =
--   number of breakpoints <= value.  Half-open buckets [lo, hi):
--     bucket(35, [30,40,45]) == 1   (the 30..40 slab)
--     bucket(40, [30,40,45]) == 2   (40 belongs to 40..45)
--   Used to index congestion-charge grids instead of nested if/else chains.
--
--   Pricing path: rather than silently return a wrong-but-plausible slab, every
--   ill-formed input throws. Breakpoints must be non-empty, finite and strictly
--   ascending; the value must be finite. (Non-ascending/empty/NaN/duplicates all
--   otherwise collapse to slab 0 or shift the index.)
bucketIndex :: (MonadThrow m, MonadCatch m, MonadIO m) => Value -> Value -> m Int
bucketIndex val (A.Array breaks) = do
  v <- getNumber' val
  bs <- mapM getNumber' (V.toList breaks)
  when (isNaN v || isInfinite v) $
    throwM $ JsonLogicError ("bucket: value is not finite: " <> show v :: String)
  when (null bs) $
    throwM $ JsonLogicError ("bucket: empty breakpoint list" :: String)
  when (any (\x -> isNaN x || isInfinite x) bs) $
    throwM $ JsonLogicError ("bucket: non-finite breakpoint in " <> show bs :: String)
  unless (and (zipWith (<) bs (drop 1 bs))) $
    throwM $ JsonLogicError ("bucket: breakpoints must be strictly ascending, got " <> show bs :: String)
  pure $ length (takeWhile (<= v) bs)
bucketIndex _ breaks =
  throwM $ JsonLogicError ("bucket expects [number, array of ascending breaks], got breaks=" <> show breaks :: String)

bucketOp :: (MonadThrow m, MonadCatch m, MonadIO m) => [Value] -> m Value
bucketOp = binaryOpJson bucketIndex

-- | arrayAt: index into an array. No clamping — an out-of-range index throws,
--   naming the index and length. For correctly sized grids the index is always
--   in range (a half-open bucket over n breakpoints yields 0..n, and the grid
--   has n+1 rows), so an out-of-range index means a malformed grid, which is
--   exactly when we want to fail loud rather than silently reuse a neighbour row.
arrayAtAt :: (MonadThrow m, MonadCatch m, MonadIO m) => Value -> Value -> m Value
arrayAtAt (A.Array arr) idxV = do
  d <- getNumber' idxV
  when (isNaN d || isInfinite d) $
    throwM $ JsonLogicError ("arrayAt: index is not finite: " <> show d :: String)
  let i = floor d :: Int
      n = V.length arr
  case arr V.!? i of
    Just x -> pure x
    Nothing -> throwM $ JsonLogicError ("arrayAt: index " <> show i <> " out of range for array of length " <> show n :: String)
arrayAtAt arr _ =
  throwM $ JsonLogicError ("arrayAt expects [array, index], got array=" <> show arr :: String)

arrayAtOp :: (MonadThrow m, MonadCatch m, MonadIO m) => [Value] -> m Value
arrayAtOp = binaryOp arrayAtAt

operations :: (MonadThrow m, MonadCatch m, MonadIO m) => Map.Map Key ([Value] -> m Value)
operations =
  Map.fromList $
    -- all in below array are checker functions i.e. checks for conditions returns bool
    map
      (DTE.first AK.fromString . DTE.second ((.) (fmap A.toJSON)))
      [ ("==", compareJson EQ),
        ("===", compareJson EQ), -- lets treat both same in haskell
        ("!=", fmap not . compareJson EQ),
        ("!==", fmap not . compareJson EQ),
        (">", compareWithAll GT),
        (">=", \a -> liftM2 (||) (compareWithAll GT a) (compareWithAll EQ a)),
        ("<", compareWithAll LT),
        ("<=", \a -> liftM2 (||) (compareWithAll LT a) (compareWithAll EQ a)),
        ("!", unaryOp (pure . toBool)),
        ("and", pure . all (not . toBool)),
        ("&&", pure . all (not . toBool)),
        ("or", pure . any (not . toBool)),
        ("||", pure . any (not . toBool)),
        ("in", binaryOp inOp)
      ]
      -- all below returns Values based or does data transformation
      <> map
        (DTE.first AK.fromString)
        [ ("?:", ifOp),
          ("if", ifOp),
          ("coalesce", coalesceOp),
          ("bucket", bucketOp),
          ("arrayAt", arrayAtOp),
          ("log", unaryOp (pure . logValue)),
          ("cat", listOpWithOutAcc (\a -> pure . concatValue a)),
          ("+", listOpJson (operateNumber (\a -> pure . (+) a)) 0),
          ("sum", listOpJson (operateNumber (\a -> pure . (+) a)) 0),
          ("*", listOpJson (operateNumber (\a -> pure . (*) a)) 1),
          ("substr", binaryOpJson (\a b -> listTrimmingOperators Drop a b)),
          ("drop", binaryOpJson (\a b -> listTrimmingOperators Drop a b)),
          ("take", binaryOpJson (\a b -> listTrimmingOperators Take a b)),
          ("min", operateNumberList (operateNumber (\a -> pure . min a)) id),
          ("max", operateNumberList (operateNumber (\a -> pure . max a)) id),
          ("merge", pure . merge),
          ("-", operateNumberList (operateNumber (\a -> pure . (-) a)) ((-1) *)),
          ("diff", operateNumberList (operateNumber (\a -> pure . (-) a)) ((-1) *)),
          ("/", binaryOpJson (\a b -> liftM2 (/) (getNumber' a) (getNumber' b))),
          ("%", binaryOpJson modOperator)
        ]
