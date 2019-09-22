{-
Copyright 2018 SlamData, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-}

module Test.Main where

import Prelude

import Control.Alt (alt)
import Control.Monad.Reader (ask)
import Control.Monad.Trans.Class (lift)
import Control.Parallel (parSequence_, parallel, sequential)
import Data.Bifunctor (lmap)
import Data.Either (Either(..), either)
import Effect (Effect)
import Effect.Aff (Aff, Milliseconds(..), delay, forkAff, joinFiber, launchAff_, never, runAff_, throwError, try)
import Effect.Aff.AVar as AVar
import Effect.Aff.Bus as Bus
import Effect.Class (liftEffect)
import Effect.Console (log)
import Effect.Exception (error)
import Effect.Ref as Ref
import Test.Assert (assertEqual', assertTrue')
import Unsafe.Coerce (unsafeCoerce)

timeout :: forall a. Milliseconds -> Aff a -> Aff a
timeout ms aff = sequential $ parallel aff `alt` parallel delayAndKill
  where delayAndKill = delay ms *> throwError (error $ "Timeout after " <> show ms)

suit :: String -> Aff Unit -> Aff Unit
suit name aff = do
  liftEffect $ log ("[Start] " <> name)
  try aff >>= case _ of
    Left err → do
      liftEffect $ log ("[Error] " <> name)
      throwError err
    Right res -> do
      liftEffect $ log ("[Done]")
      pure res

test_isKilled :: Aff Unit
test_isKilled = do
  bus <- Bus.make
  let err = error "Done"

  Bus.kill err bus

  isKilled <- Bus.isKilled bus
  liftEffect $ assertTrue' "`isKilled` immediately after `kill` results `true`" isKilled

test_kill_parallel :: Aff Unit
test_kill_parallel = do
  bus <- Bus.make

  let err = error "Done"

  parSequence_
    [ Bus.kill err bus
    , Bus.kill err bus
    , Bus.kill err bus
    ]

  isKilled <- Bus.isKilled bus
  liftEffect $ assertTrue' "killing in parallel must be safe" isKilled

test_kill_idempotent :: Aff Unit
test_kill_idempotent = do
  bus <- Bus.make

  let err = error "Done"

  Bus.kill err bus
  Bus.kill err bus
  Bus.kill err bus
  Bus.kill err bus

  isKilled <- Bus.isKilled bus
  liftEffect $ assertTrue' "killing must be idempotent" isKilled



test_kill_read :: Aff Unit
test_kill_read = do
  bus <- Bus.make

  readFiber <- forkAff $ Bus.read bus

  let err = error "Done"

  Bus.kill err bus

  isKilled <- Bus.isKilled bus
  liftEffect $ assertTrue' "`isKilled` immediately after `kill` results `true`" isKilled

  readFiberRes <- try (joinFiber readFiber)
  liftEffect $ assertEqual' "`read` from bus which is killed raises same error which was used to `kill`"
    {actual: either show absurd readFiberRes, expected: show err}

  readRes <- try (Bus.read bus)
  liftEffect $ assertEqual' "`read` from killed bus should resolve with same error which was used to `kill`"
    {actual: lmap show readRes, expected: Left $ show err}

test_consume :: Aff Unit
test_consume = do
  bus <- Bus.make
  ref ← liftEffect $ Ref.new []
  f1 ← forkAff do
    res <- try $ Bus.consume bus \res ->
      lift $ liftEffect $ void $ Ref.modify (_ <> [Right res]) ref
    void $ liftEffect $ Ref.modify (_ <> [either Left absurd res]) ref

  
  Bus.write 1 bus
  Bus.write 2 bus
  
  delay $ Milliseconds 20.0
  Bus.write 3 bus

  let err = error "Done"

  Bus.kill err bus

  joinFiber f1

  res <- liftEffect $ Ref.read ref
  liftEffect $ assertEqual' "`res` should be as expected"
    {actual: lmap show <$> res, expected: [Right 1, Right 2, Right 3, Left $ show err]}


test_consume_result :: Aff Unit
test_consume_result = do
  bus <- Bus.make
  ref ← liftEffect $ Ref.new []
  f1 ← forkAff $ Bus.consume bus \res -> ask >>= \avar -> lift $ launchAff_ $ AVar.put res avar

  
  Bus.write 7 bus
  Bus.write 9 bus

  fibRes <- joinFiber f1

  liftEffect $ assertEqual' "`consume` should return result"
    {actual: fibRes, expected: 7}


test_consumeLatest_result :: Aff Unit
test_consumeLatest_result = do
  bus <- Bus.make
  ref ← liftEffect $ Ref.new []
  f1 ← forkAff $ Bus.consumeLatest bus \res -> do
    delay $ Milliseconds 10.0
    pure $ Bus.Done res

  
  Bus.write 7 bus
  Bus.write 9 bus

  fibRes <- joinFiber f1

  liftEffect $ assertEqual' "`consumeLatest` should return result"
    {actual: fibRes, expected: 9}

test_consumeLatest :: Aff Unit
test_consumeLatest = do
  bus <- Bus.make
  ref ← liftEffect $ Ref.new []
  f1 ← forkAff do
    res <- try $ Bus.consumeLatest bus \res -> do
      delay $ Milliseconds 10.0
      void $ liftEffect $ Ref.modify (_ <> [Right res]) ref
      pure Bus.Loop
    void $ liftEffect $ Ref.modify (_ <> [either Left absurd res]) ref

  
  Bus.write 1 bus
  Bus.write 2 bus
  delay $ Milliseconds 20.0
  Bus.write 3 bus

  let err = error "Done"
  Bus.kill err bus

  joinFiber f1

  res <- liftEffect $ Ref.read ref
  liftEffect $ assertEqual' "`res` should be as expected"
    {actual: lmap show <$> res, expected: [Right 2, Left $ show err]}

test_readWrite :: Bus.BusRW Int -> Aff Unit
test_readWrite bus = do
  ref ← liftEffect $ Ref.new []
  let
    proc = do
      res ← try (Bus.read bus)
      void $ liftEffect $ Ref.modify (_ <> [res]) ref
      either (const $ pure unit) (const proc) res
  f1 ← forkAff proc
  f2 ← forkAff proc


  Bus.write 1 bus
  Bus.write 2 bus
  Bus.write 3 bus

  let err = error "Done"
  Bus.kill err bus
  
  joinFiber f1
  joinFiber f2

  res <- liftEffect $ Ref.read ref
  liftEffect $ assertEqual' "`res` should be as expected"
    {actual: lmap show <$> res, expected: [Right 1, Right 1, Right 2, Right 2, Right 3, Right 3, Left $ show err, Left $ show err]}


main :: Effect Unit
main = launchAff_ do
  let timeout' = timeout (Milliseconds 100.0)
  suit "isKilled" $ timeout' test_isKilled
  suit "kill in parallel" $ timeout' test_kill_parallel
  suit "kill is idempotent" $ timeout' test_kill_idempotent
  suit "kill and read" $ timeout' test_kill_read
  suit "consume works" $ timeout' test_consume
  suit "consume can return result" $ timeout' test_consume_result
  suit "consumeLatest works" $ timeout' test_consumeLatest
  suit "consumeLatest can return result" $ timeout' test_consumeLatest_result
  suit "Testing read/write/kill" $ timeout' $ (liftEffect Bus.make) >>= test_readWrite
