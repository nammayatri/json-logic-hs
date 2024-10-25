{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
module JsonLogic where

import Data.Aeson as A
import qualified Data.Aeson.Key as AK
import qualified Data.Aeson.KeyMap as AKM
import qualified Data.Aeson.QQ.Simple as AQ
import Data.Int (Int64)
import qualified Data.List as DL
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Foldable (foldlM)
import Data.Scientific (toBoundedInteger, toRealFloat)
import qualified Data.Text as DT
import qualified Data.Tuple.Extra as DTE
import qualified Data.Vector as V
import Control.Monad.Catch
import Control.Monad (liftM2)
import Debug.Trace (traceShowId)
import Text.Read (readMaybe)
import Control.Monad.IO.Class
import Prelude

newtype JsonLogicError = JsonLogicError
  { errorMessage :: String
  } deriving (Eq, Show)

instance Exception JsonLogicError

_poolingTest :: (MonadThrow m ,MonadCatch m, MonadIO m) => m ()
_poolingTest = do
  let tests = [AQ.aesonQQ|{ "map": [ { "var": "drivers" }, { "cat": [ { "var": "" }, { "score": { "+": [ { "*": [ { "/": [ { "var": "actualDistanceToPickup" }, 3000 ] }, 0.5 ] }, { "*": [ { "var": "safetyScore" }, 0.5 ] } ] } } ] } ] }|]
  let data_ = [AQ.aesonQQ|{ "drivers": [{"actualDistanceToPickup": 0, "safetyScore": 0, "driverPoolResult": { "driverTags": { "SafetyCohort": "Unsafe"}, "c": 1 } }, {"safetyScore": 1, "driverPoolResult": { "driverTags": { "SafetyCohort": "Safe"}, "c": 1 } }] }|]
  liftIO . print $ jsonLogicEither tests data_

_test :: (MonadThrow m ,MonadCatch m, MonadIO m) => m ()
_test = do
  let tests = [AQ.aesonQQ|{ "filter": [ { "var": "drivers" }, { "!=": [ { "var": "driverPoolResult.driverTags.SafetyCohort" }, "Unsafe" ] } ] }|]
  let data_ = [AQ.aesonQQ|{ "drivers": [{"driverPoolResult": { "driverTags": { "SafetyCohort": "Unsafe"}, "c": 1 } }, {"driverPoolResult": { "driverTags": { "SafetyCohort": "Safe"}, "c": 1 } }] }|]
  liftIO . print . A.encode =<< jsonLogic tests data_

jsonLogicEither :: Value -> Value -> Either SomeException Value
jsonLogicEither logic data_ = jsonLogic logic data_

jsonLogic :: (MonadThrow m ,MonadCatch m) => Value -> Value -> m Value
jsonLogic tests data_ =
  case tests of
    A.Object dict ->
      let operator = fst (head (AKM.toList dict))
          values = snd (head (AKM.toList dict))
       in applyOperation' operator values
    A.Array rules -> fmap A.Array $ V.mapM (flip jsonLogic data_) rules
    _ -> pure tests
  where
    isLogicOperation op = op `elem` Map.keys (operations :: Map.Map Key ([Value] -> Either SomeException Value))
    applyOperation' "filter" (A.Array values) = applyOperation "filter" (V.toList values) data_
    applyOperation' "sort" (A.Array values) = applyOperation "sort" (V.toList values) data_
    applyOperation' "map" (A.Array values) = applyOperation "map" (V.toList values) data_
    applyOperation' "var" values = do
      resolvedValues <- jsonLogicValues values data_
      applyOperation "var" resolvedValues data_
    applyOperation' operator values = do 
      if isLogicOperation operator
         then do
           resolvedValues <- jsonLogicValues values data_
           applyOperation operator resolvedValues data_
         else do
           resolvedValues <- jsonLogic values data_
           pure $ A.object [ operator .= resolvedValues]

applyOperation :: (MonadThrow m ,MonadCatch m) => Key -> [Value] -> Value -> m Value
applyOperation "var" [A.Number ind] (A.Array arr) = pure $ arr V.! (fromMaybe 0 $ toBoundedInteger ind :: Int)
applyOperation "var" [A.String ind] (A.Array arr) = pure $ arr V.! (fromMaybe 0 $ readMaybe (DT.unpack ind) :: Int) -- TODO: make it like getVar to add support for nested arrays being access using 1.1.2
applyOperation "var" [A.String ""] data_ = pure $ getVar data_ "" data_
applyOperation "var" [A.String var] data_ = pure $ getVar data_ var A.Null
applyOperation "var" [A.String var, value] data_ = pure $ getVar data_ var value
applyOperation "var" [A.Null] A.Null = pure $ A.Number 1
applyOperation "var" [] A.Null = pure $ A.Number 1
applyOperation "var" [A.Null] data_ = pure data_
applyOperation "var" [] data_ = pure data_
applyOperation "var" _ _ = throwM $ JsonLogicError ("Wrong number of arguments for var" :: String)
applyOperation "sort" values data_ = sortValues values data_
applyOperation "map" values data_ = mapIt values data_
applyOperation "filter" [A.Object var, operation] data_ = do
  case AKM.lookup (AK.fromString "var") var of
    Just (A.String varFromData) ->
      case getVar data_ varFromData A.Null of
        A.Array listToFilter -> do
          updatedData <- fmap A.Array $ V.filterM (fmap (not . toBool) . jsonLogic operation) listToFilter
          putVar varFromData updatedData data_ 
        _ -> throwM $ JsonLogicError ("wrong type of variable passed for filtering" :: String)
    _ -> throwM $ JsonLogicError ("var must be specified here" :: String)
-- applyOperation "missing" (A.Array args) data_ = List $ missing data_ args TODO: add these if required later
-- applyOperation "missing_some" [Num minReq, List args] data_ = List $ missingSome data_ (round minReq) args
applyOperation op args _ = do
  case Map.lookup op operations of
    Just fn -> fn args
    Nothing -> pure A.Null

mapIt :: (MonadThrow m ,MonadCatch m) => [Value] -> Value -> m Value
mapIt [A.String mapOn, operation] data_ = mapIt' mapOn operation data_
mapIt [A.Object mapOnVar, operation] data_ = 
  case AKM.lookup (AK.fromString "var") mapOnVar of 
    Just (A.String varFromData) -> mapIt' varFromData operation data_
    _ -> throwM $ JsonLogicError ("var must be specified here" :: String)
mapIt _ _ = throwM $ JsonLogicError ("var must be specified here" :: String)

mapIt' :: (MonadThrow m ,MonadCatch m) => DT.Text -> Value -> Value -> m Value
mapIt' mapOn operation data_ =
  case getVar data_ mapOn A.Null of
    A.Array listToFilter -> do
      updatedData <- fmap A.Array $ V.mapM (jsonLogic operation) listToFilter
      putVar mapOn updatedData data_
    _ -> throwM $ JsonLogicError ("wrong type of variable passed for mapping" :: String)

sortValues :: (MonadThrow m ,MonadCatch m) => [Value] -> Value -> m Value
sortValues [] _ = throwM $ JsonLogicError ("wrong usage of sort command" :: String)
sortValues (x:restValues) data_ = go x
  where
    go (A.Object sortWhat) = do
      case AKM.lookup (AK.fromString "var") sortWhat of
        Just (A.String varFromData) ->
          case getVar data_ varFromData A.Null of
            A.Array listToSort -> do
              updatedVar <- fmap (A.Array . V.fromList) $ sortValues' restValues (V.toList listToSort)
              putVar varFromData updatedVar data_
            _ -> throwM $ JsonLogicError ("wrong type of variable passed for sorting" :: String)
        _ -> throwM $ JsonLogicError ("wrong type of variable passed for sorting" :: String)
    go (A.String varFromData) = do
      case getVar data_ varFromData A.Null of
        A.Array listToSort -> do
          updatedVar <- fmap (A.Array . V.fromList) $ sortValues' restValues (V.toList listToSort)
          putVar varFromData updatedVar data_
        _ -> throwM $ JsonLogicError ("Wrong type of variable passed for sorting" :: String)
    go _ = throwM $ JsonLogicError ("cannot figureout what to sort broo" :: String)

sortValues' :: (MonadThrow m ,MonadCatch m) => [Value] -> [Value] -> m [Value]
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

getVar :: Value -> DT.Text -> Value -> Value
getVar (A.Object dict) varName notFound = getVarHelper dict (DT.split (== '.') varName)
  where
    getVarHelper d [key] =
      case (AKM.lookup (AK.fromString $ DT.unpack key) d) of
        Just res -> res
        Nothing -> notFound
    getVarHelper d (key : restKey) =
      case (AKM.lookup (AK.fromString $ DT.unpack key) d) of
        Just (A.Object d') -> getVarHelper d' restKey
        _ -> notFound
    getVarHelper _ _ = notFound
getVar _ _ notFound = notFound

putVar :: (MonadThrow m ,MonadCatch m) => DT.Text -> Value -> Value -> m Value
putVar key newVal (A.Object obj) = fmap A.Object $ putVarHelper obj (DT.split (== '.') key) newVal
  where
    putVarHelper :: (MonadThrow m ,MonadCatch m) => AKM.KeyMap Value -> [DT.Text] -> Value -> m (AKM.KeyMap Value)
    putVarHelper d [finalKey] val = pure $ AKM.insert (AK.fromString $ DT.unpack finalKey) val d
    putVarHelper d (key':restKey) val =
      case AKM.lookup (AK.fromString $ DT.unpack key') d of
        Just (A.Object d') -> do
          updatedRes <- putVarHelper d' restKey val
          pure $ AKM.insert (AK.fromString $ DT.unpack key') (A.Object updatedRes) d
        _ -> throwM $ JsonLogicError ("Path does not exist for putting value" :: String)
    putVarHelper _ _ _ = throwM $ JsonLogicError ("Invalid path for putting value" :: String)
putVar _ _ _ = throwM $ JsonLogicError ("putVar expects an object as the target value" :: String)

jsonLogicValues :: (MonadThrow m ,MonadCatch m) => Value -> Value -> m [Value]
jsonLogicValues (Array vals) data_ = mapM (`jsonLogic` data_) $ V.toList vals
jsonLogicValues val data_ = do
  res <- jsonLogic val data_
  pure [res]

compareJsonImpl :: Ordering -> Value -> Value -> Bool
compareJsonImpl ordering a b = do
  ( case ordering of
      EQ -> (==)
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

modOperator :: (MonadThrow m ,MonadCatch m) => Value -> Value -> m Int64
modOperator a b =
  case (a, b) of
    (A.Number aa, A.Number bb) -> do
      let aaB = toBoundedInteger aa
          bbB = toBoundedInteger bb
      case (aaB, bbB) of
        (Just aaB', Just bbB') -> pure $ mod aaB' bbB'
        _ -> throwM $ JsonLogicError ("Couldn't parse numbers" :: String)
    _ -> throwM $ JsonLogicError ("Invalid input type for mod operator a: " <> show a <> ", b: " <> show b :: String)

ifOp :: (MonadThrow m ,MonadCatch m) =>  [Value] -> m Value
ifOp [a, b, c] = pure $ if not (toBool a) then b else c
ifOp [a, b] = pure $ if not (toBool a) then b else A.Null
ifOp [a] = pure $ if not (toBool a) then a else A.Bool False
ifOp [] = pure A.Null
ifOp args = throwM $ JsonLogicError ("wrong number of args supplied, need 3 or less" <> show args :: String)

unaryOp :: (MonadThrow m ,MonadCatch m) => (Value -> m a) -> [Value] -> m a
unaryOp fn [a] = fn a
unaryOp _ _ = throwM $ JsonLogicError ("wrong number of args supplied, need 1" :: String)

logValue :: Value -> Value
logValue = traceShowId

binaryOp :: forall m a. (MonadThrow m ,MonadCatch m) => (Value -> Value -> m a) -> [Value] -> m a
binaryOp fn [a, b] = fn a b
binaryOp _ _ = throwM $ JsonLogicError ("wrong number of args supplied, need 2" :: String)

inOp :: (MonadThrow m ,MonadCatch m) => Value -> Value -> m Bool
inOp a bx = case (a, bx) of
  (a', A.Array bx') -> pure $ V.elem a' bx'
  (A.String a', A.String bx') -> pure $ a' `DT.isInfixOf` bx'
  _ -> throwM $ JsonLogicError ("failed to check if " <> show a <> " is in " <> show bx :: String)

getNumber' :: (MonadThrow m ,MonadCatch m) => Value -> m Double
getNumber' a = case a of
  A.Number aa -> pure $ toRealFloat aa
  _ -> throwM $ JsonLogicError ("expected number, got -> " <> show a :: String)
-- 
operateNumber :: (MonadThrow m ,MonadCatch m) => (Double -> Double -> m Double) -> Double -> Value -> m Double
operateNumber fn acc a = do
  a' <- getNumber' a
  acc `fn` a'

concatValue :: Value -> Value -> Value
concatValue a b = deepMerge a b

deepMerge :: Value -> Value -> Value
deepMerge (Object a) (Object b) = Object (AKM.unionWith deepMerge a b)
deepMerge (Array a) (Array b) = Array (a <> b)
deepMerge _ b = b

binaryOpJson :: forall m a. (MonadThrow m ,MonadCatch m) => ToJSON a => (Value -> Value -> m a) -> [Value] -> m Value
binaryOpJson fn = fmap A.toJSON . binaryOp fn

listOpJson :: (MonadThrow m ,MonadCatch m) => ToJSON a => (a -> Value -> m a) -> a -> [Value] -> m Value
listOpJson fn acc = fmap A.toJSON . listOp fn acc

operateNumberList :: (MonadThrow m ,MonadCatch m) => (Double -> Value -> m Double) -> (Double -> Double) -> [Value] -> m Value
operateNumberList _ _ [] = pure A.Null
operateNumberList _ onlyEntryAction [xs] = fmap (A.toJSON . onlyEntryAction) $ getNumber' xs
operateNumberList fn _ (acc : xs) = do
  accNumber <- getNumber' acc
  fmap A.toJSON $ listOp fn accNumber xs
-- 
listOp :: (MonadThrow m ,MonadCatch m) => (a -> Value -> m a) -> a -> [Value] -> m a
listOp fn acc = foldlM fn acc

listOpWithOutAcc :: (MonadThrow m ,MonadCatch m) => (Value -> Value -> m Value) -> [Value] -> m Value
listOpWithOutAcc fn vals = go vals
  where
    go (A.Object _:_) = listOp fn (A.Object AKM.empty) vals
    go _ = listOp fn (A.String "") vals

merge :: [Value] -> Value
merge = A.Array . V.fromList . concatMap getArr
  where
    getArr val = case val of
      A.Array arr -> V.toList arr
      e -> [e]

compareJson :: (MonadThrow m ,MonadCatch m) => Ordering -> [Value] -> m Bool
compareJson ord values = binaryOp (\a -> pure . compareJsonImpl ord a) values

compareAll :: (MonadThrow m ,MonadCatch m) => (Value -> Value -> Bool) -> [Value] -> m Bool
compareAll fn (x : y : xs) = liftM2 (&&) (pure $ fn x y) (compareAll fn (y : xs))
compareAll _ (_x : _xs) = pure True
compareAll _ _ = throwM $ JsonLogicError ("need atleast one element" :: String)

compareWithAll :: (MonadThrow m ,MonadCatch m) => Ordering -> [Value] -> m Bool -- TODO: probably could be improved, but wanted to write this way 😊
compareWithAll _ [] = pure False
compareWithAll _ [_x] = pure False
compareWithAll ordering xs = compareAll (compareJsonImpl ordering) xs

substr :: (MonadThrow m, MonadCatch m) => Value -> Value -> m Value
substr stringToCut cutFrom = do
  fromIndex <- round <$> getNumber' cutFrom
  elementsFromIndex fromIndex stringToCut 

elementsFromIndex :: (MonadThrow m, MonadCatch m) => Int -> Value -> m Value
elementsFromIndex n (String txt) = 
    pure . String $ DT.drop n txt
elementsFromIndex n (Array arr) = 
    pure . Array $ V.drop n arr
elementsFromIndex a b = throwM $ JsonLogicError ("wrong type of variable passed for substr " <> show (A.encode a) <> " " <> show (A.encode b) :: String)

operations :: (MonadThrow m ,MonadCatch m) => Map.Map Key ([Value] -> m Value)
operations = Map.fromList $ 
  -- all in below array are checker functions i.e. checks for conditions returns bool
  map (DTE.first AK.fromString . DTE.second ((.) (fmap A.toJSON)))
    [ ("==", compareJson EQ)
    , ("===", compareJson EQ) -- lets treat both same in haskell
    , ("!=", fmap not . compareJson EQ)
    , ("!==", fmap not . compareJson EQ)
    , (">", compareWithAll GT)
    , (">=", \a -> liftM2 (||) (compareWithAll GT a) (compareWithAll EQ a))
    , ("<", compareWithAll LT)
    , ("<=", \a -> liftM2 (||) (compareWithAll LT a) (compareWithAll EQ a))
    , ("!", unaryOp (pure . toBool))
    , ("and", pure . all (not . toBool))
    , ("&&", pure . all (not . toBool))
    , ("or", pure . any (not . toBool))
    , ("||", pure . any (not . toBool))
    , ("in", binaryOp inOp)
    ]
  -- all below returns Values based or does data transformation
  <> map (DTE.first AK.fromString)
    [ ("?:", ifOp)
    , ("if", ifOp)
    , ("log", unaryOp (pure . logValue))
    , ("cat", listOpWithOutAcc (\a -> pure . concatValue a))
    , ("+", listOpJson (operateNumber (\a -> pure . (+) a)) 0)
    , ("*", listOpJson (operateNumber (\a -> pure . (*) a)) 1)
    , ("min", operateNumberList (operateNumber (\a -> pure . min a)) id)
    , ("max", operateNumberList (operateNumber (\a -> pure . max a)) id)
    , ("substr", binaryOpJson (\a b -> substr a b))
    , ("merge", pure . merge)
    , ("-", operateNumberList (operateNumber (\a -> pure . (-) a)) ((-1) *))
    , ("/", binaryOpJson (\a b -> liftM2 (/) (getNumber' a) (getNumber' b)))
    , ("%", binaryOpJson modOperator)
    ]
