{-# LANGUAGE QuasiQuotes #-}
module JsonLogic
  ( jsonLogic
  )
where

import Data.Aeson as A
import qualified Data.Aeson.Key as AK
import qualified Data.Aeson.KeyMap as AKM
import qualified Data.Aeson.QQ.Simple as AQ
import Data.Int (Int64)
import qualified Data.List as DL
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Data.Scientific (toBoundedInteger, toRealFloat)
import qualified Data.Text as DT
import qualified Data.Tuple.Extra as DTE
import qualified Data.Vector as V
import Debug.Trace (traceShowId)
import Text.Read (readMaybe)
import Prelude

-- an example of filter function
_poolingTest :: IO ()
_poolingTest = do
  let tests = [AQ.aesonQQ|{ "map": [ { "var": "drivers" }, { "cat": [ { "var": "" }, { "if": [ { "==": [ { "var": "driverPoolResult.driverTags.SafetyCohort" }, "Safe" ] }, { "safetyScore": 0 }, { "if": [ { "==": [ { "var": "driverPoolResult.driverTags.SafetyCohort" }, "Problematic" ] }, { "safetyScore": 0.5 }, { "if": [ { "==": [ { "var": "driverPoolResult.driverTags.SafetyCohort" }, "Unsafe" ] }, { "safetyScore": 1 }, { "safetyScore": 0.25 } ]} ] } ] } ] } ] }|]

  let data_ = [AQ.aesonQQ|{ "drivers": [{"customerInfo": {"gender": "MALE"}, "driverPoolResult": {"driverTags": {"SafetyCohort": "Unsafe"}}}]}|]
  -- print tests
  print . A.encode $ jsonLogic tests data_

_test :: IO ()
_test = do
  -- let tests = [AQ.aesonQQ|{ "map":["nestedIntegers",{ "cat": [{ "var": ""}, { "if": [ { "==": [{"var": "a.b"}, 1] }, { "a": { "score": 10 } }, { "a" : { "score": 20 } } ]}]}] }|]
  let tests = [AQ.aesonQQ|{ "filter": [ { "var": "drivers" }, { "!=": [ { "var": "driverPoolResult.driverTags.SafetyCohort" }, "Unsafe" ] } ] }|]
  let data_ = [AQ.aesonQQ|{ "drivers": [{"driverPoolResult": { "driverTags": { "SafetyCohort": "Unsafe"}, "c": 1 } }, {"driverPoolResult": { "driverTags": { "SafetyCohort": "Safe"}, "c": 1 } }] }|]
  print . A.encode $  jsonLogic tests data_

jsonLogic :: Value -> Value -> Value
jsonLogic tests data_ =
  case tests of
    A.Object dict ->
      let operator = fst (head (AKM.toList dict))
          values = snd (head (AKM.toList dict))
       in applyOperation' operator values
    A.Array rules -> A.Array $ V.map (flip jsonLogic data_) rules
    _ -> tests
  where
    isLogicOperation op = op `elem` Map.keys operations
    applyOperation' "filter" (A.Array values) = applyOperation "filter" (V.toList values) data_
    applyOperation' "sort" (A.Array values) = applyOperation "sort" (V.toList values) data_
    applyOperation' "map" (A.Array values) = applyOperation "map" (V.toList values) data_
    applyOperation' "var" values = applyOperation "var" (jsonLogicValues values data_) data_
    applyOperation' operator values = do 
      if isLogicOperation operator
         then applyOperation operator (jsonLogicValues values data_) data_
         else A.object [ operator .= jsonLogic values data_]

applyOperation :: Key -> [Value] -> Value -> Value
applyOperation "var" [A.Number ind] (A.Array arr) = arr V.! (fromMaybe 0 $ toBoundedInteger ind :: Int)
applyOperation "var" [A.String ind] (A.Array arr) = arr V.! (fromMaybe 0 $ readMaybe (DT.unpack ind) :: Int) -- TODO: make it like getVar to add support for nested arrays being access using 1.1.2
applyOperation "var" [A.String ""] data_ = getVar data_ "" data_
applyOperation "var" [A.String var] data_ = getVar data_ var A.Null
applyOperation "var" [A.String var, value] data_ = getVar data_ var value
applyOperation "var" [A.Null] A.Null = A.Number 1
applyOperation "var" [] A.Null = A.Number 1
applyOperation "var" [A.Null] data_ = data_
applyOperation "var" [] data_ = data_
applyOperation "var" _ _ = error "Wrong number of arguments for var"
applyOperation "sort" values data_ = sortValues values data_
applyOperation "map" values data_ = mapIt values data_
applyOperation "filter" [A.Object var, operation] data_ = do
  case AKM.lookup (AK.fromString "var") var of
    Just (A.String varFromData) ->
      case getVar data_ varFromData A.Null of
        A.Array listToFilter -> do
          let updatedData = A.Array $ V.filter (not . toBool . jsonLogic operation) listToFilter
          putVar varFromData updatedData data_ 
        _ -> error "wrong type of variable passed for filtering"
    _ -> error "var must be specified here"
-- applyOperation "missing" (A.Array args) data_ = List $ missing data_ args TODO: add these if required later
-- applyOperation "missing_some" [Num minReq, List args] data_ = List $ missingSome data_ (round minReq) args
applyOperation op args _ = fromMaybe Null $ Map.lookup op operations <*> pure args

mapIt :: [Value] -> Value -> Value
mapIt [A.String mapOn, operation] data_ = mapIt' mapOn operation data_
mapIt [A.Object mapOnVar, operation] data_ = 
  case AKM.lookup (AK.fromString "var") mapOnVar of 
    Just (A.String varFromData) -> mapIt' varFromData operation data_
    _ -> error "var must be specified here"
mapIt _ _ = error "var must be specified here"

mapIt' :: DT.Text -> Value -> Value -> Value
mapIt' mapOn operation data_ =
  case getVar data_ mapOn A.Null of
    A.Array listToFilter -> do
      let updatedData = A.Array $ V.map (jsonLogic operation) listToFilter
      putVar mapOn updatedData data_
    _ -> error "wrong type of variable passed for mapping"

sortValues :: [Value] -> Value -> Value
sortValues [] _ = error "wrong usage of sort command"
sortValues (x:restValues) data_ = go x
  where
    go (A.Object sortWhat) = do
      case AKM.lookup (AK.fromString "var") sortWhat of
        Just (A.String varFromData) ->
          case getVar data_ varFromData A.Null of
            A.Array listToSort -> do
              let updatedVar = A.Array . V.fromList $ sortValues' restValues (V.toList listToSort)
              putVar varFromData updatedVar data_
            _ -> error "wrong type of variable passed for sorting"
        _ -> error "wrong type of variable passed for sorting"
    go (A.String varFromData) = do
      case getVar data_ varFromData A.Null of
        A.Array listToSort -> do
          let updatedVar = A.Array . V.fromList $ sortValues' restValues (V.toList listToSort)
          putVar varFromData updatedVar data_
        _ -> error "Wrong type of variable passed for sorting"
    go _ = error "cannot figureout what to sort broo"

sortValues' :: [Value] -> [Value] -> [Value]
sortValues' [] listToSort = map A.Object . DL.sort $ map getObjects listToSort
sortValues' [A.String on] listToSort = map A.Object . sortValuesOn on $ map getObjects listToSort
sortValues' [A.Object on] listToSort = do
  case AKM.lookup (AK.fromString "var") on of 
    Just (A.String on') -> map A.Object . sortValuesOn on' $ map getObjects listToSort
    _ -> error $ "on part of sort command contains unsupported type for var field, should be string, on command: " <> show on
sortValues' on _ = error $ "wrong type of data used to pass 'on' field for sortOn, should be String or {\"var\":\"String\"}, on command: " <> show on

getObjects :: Value -> AKM.KeyMap Value
getObjects (A.Object obj) = obj
getObjects _ = error "needs to be object type"

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

putVar :: DT.Text -> Value -> Value -> Value
putVar key newVal (A.Object obj) = A.Object $ putVarHelper obj (DT.split (== '.') key) newVal
  where
    putVarHelper :: AKM.KeyMap Value -> [DT.Text] -> Value -> AKM.KeyMap Value
    putVarHelper d [finalKey] val = AKM.insert (AK.fromString $ DT.unpack finalKey) val d
    putVarHelper d (key':restKey) val = 
      case AKM.lookup (AK.fromString $ DT.unpack key') d of
        Just (A.Object d') -> AKM.insert (AK.fromString $ DT.unpack key') (A.Object $ putVarHelper d' restKey val) d
        _ -> error "Path does not exist for putting value"
    putVarHelper _ _ _ = error "Invalid path for putting value"
putVar _ _ _ = error "putVar expects an object as the target value"

jsonLogicValues :: Value -> Value -> [Value]
jsonLogicValues (Array vals) data_ = map (`jsonLogic` data_) $ V.toList vals
jsonLogicValues val data_ = [jsonLogic val data_]

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

modOperator :: Value -> Value -> Int64
modOperator a b =
  case (a, b) of
    (A.Number aa, A.Number bb) -> do
      let aaB = toBoundedInteger aa
          bbB = toBoundedInteger bb
      case (aaB, bbB) of
        (Just aaB', Just bbB') -> mod aaB' bbB'
        _ -> error "Couldn't parse numbers"
    _ -> error $ "Invalid input type for mod operator a: " <> show a <> ", b: " <> show b

ifOp :: [Value] -> Value
ifOp [a, b, c] = if not (toBool a) then b else c
ifOp [a, b] = if not (toBool a) then b else A.Null
ifOp [a] = if not (toBool a) then a else A.Bool False
ifOp [] = A.Null
ifOp args = error $ "wrong number of args supplied, need 3 or less" <> show args

unaryOp :: (Value -> a) -> [Value] -> a
unaryOp fn [a] = fn a
unaryOp _ _ = error "wrong number of args supplied, need 1"

logValue :: Value -> Value
logValue = traceShowId

binaryOp :: (Value -> Value -> a) -> [Value] -> a
binaryOp fn [a, b] = fn a b
binaryOp _ _ = error "wrong number of args supplied, need 2"

inOp :: Value -> Value -> Bool
inOp a bx = case (a, bx) of
  (a', A.Array bx') -> V.elem a' bx'
  (A.String a', A.String bx') -> a' `DT.isInfixOf` bx'
  _ -> error $ "failed to check if " <> show a <> " is in " <> show bx

getNumber' :: Value -> Double
getNumber' a = case a of
  A.Number aa -> toRealFloat aa
  _ -> error $ "expected number, got -> " <> show a

operateNumber :: (Double -> Double -> Double) -> Double -> Value -> Double
operateNumber fn acc a =
  acc `fn` getNumber' a

concatValue :: Value -> Value -> Value
concatValue a b = deepMerge a b

deepMerge :: Value -> Value -> Value
deepMerge (Object a) (Object b) = Object (AKM.unionWith deepMerge a b)
deepMerge (Array a) (Array b) = Array (a <> b)
deepMerge _ b = b

binaryOpJson :: ToJSON a => (Value -> Value -> a) -> [Value] -> Value
binaryOpJson fn = A.toJSON . binaryOp fn

listOpJson :: ToJSON a => (a -> Value -> a) -> a -> [Value] -> Value
listOpJson fn acc = A.toJSON . listOp fn acc

operateNumberList :: (Double -> Value -> Double) -> (Double -> Double) -> [Value] -> Value
operateNumberList _ _ [] = A.Null
operateNumberList _ onlyEntryAction [xs] = A.toJSON . onlyEntryAction $ getNumber' xs
operateNumberList fn _ (acc : xs) = A.toJSON $ listOp fn (getNumber' acc) xs

listOp :: (a -> Value -> a) -> a -> [Value] -> a
listOp fn acc = DL.foldl' fn acc

listOpWithOutAcc :: (Value -> Value -> Value) -> [Value] -> Value
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

compareJson :: Ordering -> [Value] -> Bool
compareJson = binaryOp . compareJsonImpl

compareAll :: (Value -> Value -> Bool) -> [Value] -> Bool
compareAll fn (x : y : xs) = fn x y && compareAll fn (y : xs)
compareAll _ (_x : _xs) = True
compareAll _ _ = error "need atleast one element"

compareWithAll :: Ordering -> [Value] -> Bool -- TODO: probably could be improved, but wanted to write this way 😊
compareWithAll _ [] = False
compareWithAll _ [_x] = False
compareWithAll ordering xs = compareAll (compareJsonImpl ordering) xs

operations :: Map.Map Key ([Value] -> Value)
operations = Map.fromList $ 
  -- all in below array are checker functions i.e. checks for conditions returns bool
  map (DTE.first AK.fromString . DTE.second ((.) A.toJSON))
    [ ("==", compareJson EQ)
    , ("===", compareJson EQ) -- lets treat both same in haskell
    , ("!=", not . compareJson EQ)
    , ("!==", not . compareJson EQ)
    , (">", compareWithAll GT)
    , (">=", \a -> compareWithAll GT a || compareWithAll EQ a)
    , ("<", compareWithAll LT)
    , ("<=", \a -> compareWithAll LT a || compareWithAll EQ a)
    , ("!", unaryOp toBool)
    , ("and", all (not . toBool))
    , ("&&", all (not . toBool))
    , ("or", any (not . toBool))
    , ("||", any (not . toBool))
    , ("in", binaryOp inOp)
    ]
  -- all below returns Values based or does data transformation
  <> map (DTE.first AK.fromString)
    [ ("?:", ifOp)
    , ("if", ifOp)
    , ("log", unaryOp logValue)
    , ("cat", listOpWithOutAcc concatValue)
    , ("+", listOpJson (operateNumber (+)) 0)
    , ("*", listOpJson (operateNumber (*)) 1)
    , ("min", operateNumberList (operateNumber min) id)
    , ("max", operateNumberList (operateNumber max) id)
    , ("merge", merge)
    , ("-", operateNumberList (operateNumber (-)) ((-1) *))
    , ("/", binaryOpJson (\a b -> getNumber' a / getNumber' b))
    , ("%", binaryOpJson modOperator) 
    ] 
