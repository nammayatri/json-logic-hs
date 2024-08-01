{-# LANGUAGE QuasiQuotes #-}
module JsonLogic
  ( jsonLogic
  , Value(..)
  ) where

import qualified Data.Map as Map
import Data.Aeson as A
import Data.Int (Int64)
import qualified Data.List as DL
import qualified Data.Text as DT
import Prelude
import qualified Data.Tuple.Extra as DTE
import qualified Data.Aeson.KeyMap as AKM
import Data.Scientific (toBoundedInteger, toRealFloat)
import qualified Data.Aeson.QQ.Simple as AQ
import qualified Data.Aeson.Key as AK
import qualified Data.Vector as V
import Text.Read (readMaybe)
import Data.Maybe (fromMaybe)
import Debug.Trace (traceShowId)

-- an example of filter function
_test :: IO ()
_test = do
  let tests = [AQ.aesonQQ|{"filter":[{"var":"nestedIntegers"},{ "!" : {"in":[{"var":"a"},[null, 1, 3]]}}]}|]
  let data_ = [AQ.aesonQQ|{ "nestedIntegers": [{"b": 1}, { "a": 2}, {"a" :3}, {"a": 4}, { "a": 1}]}|]
  print tests
  print $ jsonLogic tests data_

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
    applyOperation' "filter" (A.Array values) = applyOperation "filter" (V.toList values) data_
    applyOperation' operator values = applyOperation operator (jsonLogicValues values data_) data_

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
applyOperation "filter" [A.Object var, operation] data_ = do
  case AKM.lookup (AK.fromString "var") var of 
    Just (A.String varFromData) -> 
      case getVar data_ varFromData A.Null of
        A.Array listToFilter -> A.Array $ V.filter (not . toBool . jsonLogic operation . traceShowId) listToFilter
        _ -> error "wrong type of variable passed for filtering"
    _ -> error "var must be specified here"

-- applyOperation "missing" (A.Array args) data_ = List $ missing data_ args TODO: add these if required later
-- applyOperation "missing_some" [Num minReq, List args] data_ = List $ missingSome data_ (round minReq) args
applyOperation op args _ = fromMaybe Null $ Map.lookup op operations <*> pure args

getVar :: Value -> DT.Text -> Value -> Value
getVar (A.Object dict) varName notFound = getVarHelper dict (DT.split (== '.') varName)
  where
    getVarHelper d [key] =
      case (AKM.lookup (AK.fromString $ DT.unpack key) d) of
        Just res -> res
        Nothing -> notFound
    getVarHelper d (key:restKey) = 
      case (AKM.lookup (AK.fromString $ DT.unpack key) d) of
        Just (A.Object d') -> getVarHelper d' restKey
        _ -> notFound
    getVarHelper _ _ = notFound
getVar _ _ notFound = notFound

jsonLogicValues :: Value -> Value -> [Value]
jsonLogicValues (Array vals) data_ = map (`jsonLogic` data_) $ V.toList vals
jsonLogicValues val data_ = [jsonLogic val data_]

compareJsonImpl :: Ordering -> Value -> Value -> Bool
compareJsonImpl ordering a b = do
  (case ordering of 
    EQ -> (==)
    LT -> (<)
    GT -> (>)) a b
      
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

concatinateStrings :: DT.Text -> Value -> DT.Text
concatinateStrings acc a = 
  acc <> case a of 
    A.String aa -> aa
    A.Number _ -> DT.pack . show $ getNumber' a
    _ -> DT.pack $ show a

binaryOpJson :: ToJSON a => (Value -> Value -> a) -> [Value] -> Value
binaryOpJson fn = A.toJSON . binaryOp fn

listOpJson :: ToJSON a => (a -> Value -> a) -> a -> [Value] -> Value
listOpJson fn acc = A.toJSON . listOp fn acc

operateNumberList :: (Double -> Value -> Double) -> (Double -> Double) -> [Value] -> Value
operateNumberList _ _ [] = A.Null
operateNumberList _ onlyEntryAction [xs] = A.toJSON . onlyEntryAction $ getNumber' xs
operateNumberList fn _ (acc:xs) = A.toJSON $ listOp fn (getNumber' acc) xs

listOp :: (a -> Value -> a) -> a -> [Value] -> a
listOp fn acc = DL.foldl' fn acc

merge :: [Value] -> Value
merge = A.Array . V.fromList . concatMap getArr
  where
    getArr val = case val of
        A.Array arr -> V.toList arr
        e -> [e]

compareJson :: Ordering -> [Value] -> Bool
compareJson = binaryOp . compareJsonImpl

compareAll :: (Value -> Value -> Bool) -> [Value] -> Bool
compareAll fn (x:y:xs) = fn x y && compareAll fn (y:xs)
compareAll _ (_x:_xs) = True
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
    -- , ("!!", Booll . all toBool) -- TODO: will implement later if required 
    , ("and", all (not . toBool))
    , ("or", any (not . toBool))
    , ("in", binaryOp inOp)
    ]
  -- all below returns Values based or does data transformation
  <> map (DTE.first AK.fromString)
    [ ("?:", ifOp)
    , ("if", ifOp)
    , ("log", unaryOp logValue)
    , ("cat", listOpJson concatinateStrings (DT.pack ""))
    , ("+", listOpJson (operateNumber (+)) 0)
    , ("*", listOpJson (operateNumber (*)) 1)
    , ("min", operateNumberList (operateNumber min) id)
    , ("max", operateNumberList (operateNumber max) id)
    , ("merge", merge)
    , ("-", operateNumberList (operateNumber (-)) ((-1) *))
    , ("/", binaryOpJson (\a b -> getNumber' a / getNumber' b))
    , ("%", binaryOpJson modOperator) 
    ] 
